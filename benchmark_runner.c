#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#if defined(_WIN32)
#include <windows.h>
#define popen _popen
#define pclose _pclose
#else
#include <sys/resource.h>
#include <sys/wait.h>
#endif

#define SPEC_VERSION "bench-v1"
#define DEFAULT_SEED 20260415u
#define DEFAULT_REPEAT 3
#define EXPECTED_FAILURE_ERRORS 4
#define DELETE_ESTIMATED_NORMALIZED 0.70

#define REF_ID 1600000.0
#define REF_UK 750000.0
#define REF_SCAN 160.0
#define REF_INSERT 140000.0
#define REF_UPDATE 220000.0
#define REF_DELETE 120000.0
#define REF_UTIL_PROXY 0.03
#define REF_UTIL_MEMTRACK 0.08

#if defined(_WIN32)
#define SQLPROCESSOR_CMD "sqlsprocessor.exe"
#define BENCH_GENERATOR_CMD "bench_workload_generator.exe"
#else
#define SQLPROCESSOR_CMD "./sqlsprocessor"
#define BENCH_GENERATOR_CMD "./bench_workload_generator"
#endif

typedef struct {
    const char *profile;
    int rows;
    int update_rows;
    int delete_rows;
    int mixed_ops;
    unsigned int seed;
    int repeat;
    int memtrack;
    int report_only;
} RunnerOptions;

typedef struct {
    double insert;
    double id_select;
    double uk_email_select;
    double uk_phone_select;
    double scan_select;
    double update;
    double delete_op;
} Throughput;

typedef struct {
    Throughput throughput;
    double peak_heap_requested_bytes;
    int delete_measured;
} IterationResult;

static int profile_rows(const char *profile) {
    if (strcmp(profile, "smoke") == 0) return 10000;
    if (strcmp(profile, "regression") == 0) return 100000;
    return 1000000;
}

static int profile_update_rows(const char *profile) {
    if (strcmp(profile, "score") == 0) return 100000;
    return profile_rows(profile);
}

static int profile_delete_rows(const char *profile) {
    if (strcmp(profile, "score") == 0) return 100000;
    return profile_rows(profile);
}

static int profile_ops(const char *profile) {
    if (strcmp(profile, "smoke") == 0) return 20000;
    if (strcmp(profile, "regression") == 0) return 100000;
    return 500000;
}

static double now_seconds(void) {
#if defined(_WIN32)
    LARGE_INTEGER freq;
    LARGE_INTEGER counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / (double)freq.QuadPart;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
#endif
}

static void trim_newline(char *s) {
    size_t len;
    if (!s) return;
    len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[--len] = '\0';
    }
}

static int contains_token(const char *text, const char *token) {
    return text && token && strstr(text, token) != NULL;
}

static int count_token(const char *text, const char *token) {
    int count = 0;
    const char *p = text;
    size_t tlen;
    if (!text || !token) return 0;
    tlen = strlen(token);
    while ((p = strstr(p, token)) != NULL) {
        count++;
        p += tlen;
    }
    return count;
}

static int ensure_dir(const char *path) {
#if defined(_WIN32)
    int rc = mkdir(path);
#else
    int rc = mkdir(path, 0755);
#endif
    if (rc == 0 || errno == EEXIST) return 1;
    return 0;
}

static int run_command_capture(const char *cmd, char *out, size_t out_size, double *elapsed_sec, int *exit_code) {
    FILE *pipe;
    char buf[4096];
    size_t used = 0;
    int status;
    double start;

    if (!out || out_size == 0) return 0;
    out[0] = '\0';

    pipe = popen(cmd, "r");
    if (!pipe) return 0;

    start = now_seconds();
    while (fgets(buf, sizeof(buf), pipe)) {
        size_t len = strlen(buf);
        if (used + len + 1 < out_size) {
            memcpy(out + used, buf, len);
            used += len;
            out[used] = '\0';
        }
    }
    status = pclose(pipe);
    if (elapsed_sec) *elapsed_sec = now_seconds() - start;

    if (exit_code) {
#if defined(_WIN32)
        *exit_code = status;
#else
        if (WIFEXITED(status)) *exit_code = WEXITSTATUS(status);
        else *exit_code = 1;
#endif
    }
    return 1;
}

static double parse_memtrack_peak(const char *output) {
    const char *p = output;
    double peak = 0.0;
    while ((p = strstr(p, "[memtrack] peak_heap_requested_bytes=")) != NULL) {
        double value = 0.0;
        if (sscanf(p, "[memtrack] peak_heap_requested_bytes=%lf", &value) == 1 && value > peak) {
            peak = value;
        }
        p += 10;
    }
    return peak;
}

static int parse_benchmark_output(const char *output, Throughput *thru) {
    const char *p;
    int inserted = 0;
    double insert_sec = 0.0;
    int id_q = 0, email_q = 0, phone_q = 0, scan_q = 0;
    double id_sec = 0.0, email_sec = 0.0, phone_sec = 0.0, scan_sec = 0.0;

    memset(thru, 0, sizeof(*thru));

    p = strstr(output, "inserted records through INSERT path:");
    if (!p || sscanf(p, "inserted records through INSERT path: %d (%lf sec)", &inserted, &insert_sec) != 2) {
        p = strstr(output, "bulk-loaded records through RowRef path:");
        if (!p || sscanf(p, "bulk-loaded records through RowRef path: %d (%lf sec)", &inserted, &insert_sec) != 2) {
            return 0;
        }
    }

    p = strstr(output, "id SELECT using B+ tree:");
    if (!p || sscanf(p, "id SELECT using B+ tree: %lf sec total (%d queries", &id_sec, &id_q) != 2) {
        return 0;
    }

    p = strstr(output, "email(UK) SELECT using B+ tree:");
    if (!p || sscanf(p, "email(UK) SELECT using B+ tree: %lf sec total (%d queries", &email_sec, &email_q) != 2) {
        return 0;
    }

    p = strstr(output, "phone(UK) SELECT using B+ tree:");
    if (!p || sscanf(p, "phone(UK) SELECT using B+ tree: %lf sec total (%d queries", &phone_sec, &phone_q) != 2) {
        phone_sec = email_sec;
        phone_q = email_q;
    }

    p = strstr(output, "name SELECT using linear scan:");
    if (!p || sscanf(p, "name SELECT using linear scan: %lf sec total (%d queries", &scan_sec, &scan_q) != 2) {
        return 0;
    }

    thru->insert = insert_sec > 0.0 ? (double)inserted / insert_sec : 0.0;
    thru->id_select = id_sec > 0.0 ? (double)id_q / id_sec : 0.0;
    thru->uk_email_select = email_sec > 0.0 ? (double)email_q / email_sec : 0.0;
    thru->uk_phone_select = phone_sec > 0.0 ? (double)phone_q / phone_sec : 0.0;
    thru->scan_select = scan_sec > 0.0 ? (double)scan_q / scan_sec : 0.0;
    return 1;
}

static int reset_workload_csv_header(void) {
    FILE *src = fopen("jungle_benchmark_users.csv", "r");
    FILE *dst = fopen("jungle_workload_users.csv", "w");
    char line[4096];
    if (!src || !dst) {
        if (src) fclose(src);
        if (dst) fclose(dst);
        return 0;
    }
    if (!fgets(line, sizeof(line), src)) {
        fclose(src);
        fclose(dst);
        return 0;
    }
    trim_newline(line);
    fprintf(dst, "%s\n", line);
    fclose(src);
    fclose(dst);
    return 1;
}

static int run_sql_file(const char *sql_file, int memtrack, double *elapsed, int *exit_code, char *output, size_t output_size) {
    char cmd[1024];
    if (memtrack) {
#if defined(_WIN32)
        snprintf(cmd, sizeof(cmd), "set BENCH_MEMTRACK_REPORT=1&& " SQLPROCESSOR_CMD " --quiet %s 2>&1", sql_file);
#else
        snprintf(cmd, sizeof(cmd), "BENCH_MEMTRACK_REPORT=1 " SQLPROCESSOR_CMD " --quiet %s 2>&1", sql_file);
#endif
    } else {
        snprintf(cmd, sizeof(cmd), SQLPROCESSOR_CMD " --quiet %s 2>&1", sql_file);
    }
    return run_command_capture(cmd, output, output_size, elapsed, exit_code);
}

static double max_live_payload_bytes(int rows) {
    FILE *f = fopen("jungle_benchmark_users.csv", "r");
    char line[4096];
    int count = 0;
    double total = 0.0;
    if (!f) return 0.0;
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return 0.0;
    }
    while (count < rows && fgets(line, sizeof(line), f)) {
        trim_newline(line);
        total += (double)strlen(line);
        count++;
    }
    fclose(f);
    return total;
}

static double ru_maxrss_bytes(void) {
#if defined(_WIN32)
    return 0.0;
#else
    struct rusage usage;
    if (getrusage(RUSAGE_CHILDREN, &usage) != 0) return 0.0;
#if defined(__APPLE__)
    return (double)usage.ru_maxrss;
#else
    return (double)usage.ru_maxrss * 1024.0;
#endif
#endif
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double median(double *arr, int n) {
    double *copy;
    double m;
    if (n <= 0) return 0.0;
    copy = (double *)malloc((size_t)n * sizeof(double));
    if (!copy) return 0.0;
    memcpy(copy, arr, (size_t)n * sizeof(double));
    qsort(copy, (size_t)n, sizeof(double), cmp_double);
    if (n % 2 == 0) m = (copy[n / 2 - 1] + copy[n / 2]) / 2.0;
    else m = copy[n / 2];
    free(copy);
    return m;
}

static double normalize(double value, double ref) {
    if (ref <= 0.0 || value <= 0.0) return 0.0;
    if (value >= ref) return 1.0;
    return value / ref;
}

static void stage_begin(const char *label) {
    printf("[progress] %s ...\n", label);
    fflush(stdout);
}

static void stage_end(const char *label, int ok, double elapsed_sec) {
    if (elapsed_sec > 0.0) {
        printf("[progress] %s %s (%.2f sec)\n", label, ok ? "done" : "failed", elapsed_sec);
    } else {
        printf("[progress] %s %s\n", label, ok ? "done" : "failed");
    }
    fflush(stdout);
}

static void parse_args(int argc, char **argv, RunnerOptions *opt) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--profile") == 0 && i + 1 < argc) opt->profile = argv[++i];
        else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) opt->seed = (unsigned int)strtoul(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) opt->repeat = atoi(argv[++i]);
        else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) opt->rows = atoi(argv[++i]);
        else if (strcmp(argv[i], "--preload") == 0 && i + 1 < argc) opt->rows = atoi(argv[++i]);
        else if (strcmp(argv[i], "--update-rows") == 0 && i + 1 < argc) opt->update_rows = atoi(argv[++i]);
        else if (strcmp(argv[i], "--delete-rows") == 0 && i + 1 < argc) opt->delete_rows = atoi(argv[++i]);
        else if (strcmp(argv[i], "--ops") == 0 && i + 1 < argc) opt->mixed_ops = atoi(argv[++i]);
        else if (strcmp(argv[i], "--memtrack") == 0) opt->memtrack = 1;
        else if (strcmp(argv[i], "--report-only") == 0) opt->report_only = 1;
    }

    if (opt->rows <= 0) opt->rows = profile_rows(opt->profile);
    if (opt->update_rows <= 0) opt->update_rows = profile_update_rows(opt->profile);
    if (opt->delete_rows <= 0) opt->delete_rows = profile_delete_rows(opt->profile);
    if (opt->update_rows > opt->rows) opt->update_rows = opt->rows;
    if (opt->delete_rows > opt->rows) opt->delete_rows = opt->rows;
    if (opt->mixed_ops <= 0) opt->mixed_ops = profile_ops(opt->profile);
    if (opt->repeat <= 0) opt->repeat = DEFAULT_REPEAT;
}

static int regenerate_md_from_raw(void) {
    FILE *raw = fopen("artifacts/bench/report.raw", "r");
    FILE *md = fopen("artifacts/bench/report.md", "w");
    char line[512];
    char score[128] = "N/A";
    char correctness[32] = "false";
    char delete_mode[64] = "estimated";

    if (!raw || !md) {
        if (raw) fclose(raw);
        if (md) fclose(md);
        return 0;
    }

    while (fgets(line, sizeof(line), raw)) {
        trim_newline(line);
        if (strncmp(line, "score_total=", 12) == 0) strncpy(score, line + 12, sizeof(score) - 1);
        else if (strncmp(line, "correctness_pass=", 17) == 0) strncpy(correctness, line + 17, sizeof(correctness) - 1);
        else if (strncmp(line, "delete_mode=", 12) == 0) strncpy(delete_mode, line + 12, sizeof(delete_mode) - 1);
    }

    fprintf(md, "# Benchmark Report\n\n");
    fprintf(md, "- correctness_pass: `%s`\n", correctness);
    fprintf(md, "- delete_mode: `%s`\n", delete_mode);
    fprintf(md, "- score_total: `%s`\n\n", score);
    fprintf(md, "See `artifacts/bench/report.json` for full metrics.\n");

    fclose(raw);
    fclose(md);
    return 1;
}

int main(int argc, char **argv) {
    RunnerOptions opt;
    IterationResult *iters = NULL;
    double *id_vals = NULL, *email_vals = NULL, *phone_vals = NULL, *scan_vals = NULL;
    double *insert_vals = NULL, *update_vals = NULL, *delete_vals = NULL;
    double peak_heap = 0.0;
    int correctness_pass = 1;
    char fail_reasons[16][256];
    int fail_count = 0;
    double score_total = 0.0;
    double util_raw = 0.0;
    double util_norm = 0.0;
    double util_ref = REF_UTIL_PROXY;
    double n_id, n_uk_email, n_uk_phone, n_uk, n_scan, n_insert, n_update, n_delete;
    double s_select, s_thru, s_util;
    double t_id = 0.0, t_email = 0.0, t_phone = 0.0, t_scan = 0.0, t_insert = 0.0, t_update = 0.0, t_delete = 0.0;
    double peak_process_bytes;
    double live_payload;
    char delete_mode[16] = "measured";
    char util_mode[16] = "proxy";
    char *output = NULL;
    size_t output_size = 1u << 20;
    char cmd[1024];
    int rc;
    double total_start = now_seconds();
    double total_elapsed = 0.0;

    opt.profile = "score";
    opt.rows = 0;
    opt.update_rows = 0;
    opt.delete_rows = 0;
    opt.mixed_ops = 0;
    opt.seed = DEFAULT_SEED;
    opt.repeat = DEFAULT_REPEAT;
    opt.memtrack = 0;
    opt.report_only = 0;
    parse_args(argc, argv, &opt);
    output = (char *)malloc(output_size);
    if (!output) {
        fprintf(stderr, "[error] memory allocation failed\n");
        return 1;
    }

    ensure_dir("artifacts");
    ensure_dir("artifacts/bench");

    if (opt.report_only) {
        if (!regenerate_md_from_raw()) {
            fprintf(stderr, "[error] failed to regenerate report.md from report.raw\n");
            free(output);
            return 1;
        }
        printf("[ok] regenerated artifacts/bench/report.md\n");
        free(output);
        return 0;
    }

    if (opt.memtrack) {
#if defined(_WIN32)
        rc = system("gcc -O2 -fdiagnostics-color=always -g -DBENCH_MEMTRACK main.c -o sqlsprocessor.exe");
#else
        rc = system("make -B build CFLAGS='-O2 -fdiagnostics-color=always -g -DBENCH_MEMTRACK'");
#endif
    } else {
#if defined(_WIN32)
        rc = system("gcc -O2 -fdiagnostics-color=always -g main.c -o sqlsprocessor.exe");
#else
        rc = system("make -B build CFLAGS='-O2 -fdiagnostics-color=always -g'");
#endif
    }
    if (rc != 0) {
        fprintf(stderr, "[error] build failed\n");
        free(output);
        return 1;
    }

    snprintf(cmd, sizeof(cmd),
             BENCH_GENERATOR_CMD " --profile %s --seed %u --preload %d --update-rows %d --delete-rows %d --ops %d --output-dir generated_sql",
             opt.profile, opt.seed, opt.rows, opt.update_rows, opt.delete_rows, opt.mixed_ops);
    if (system(cmd) != 0) {
        fprintf(stderr, "[error] workload generation failed\n");
        return 1;
    }

    if (!reset_workload_csv_header()) {
        fprintf(stderr, "[error] failed to reset workload CSV\n");
        return 1;
    }

    snprintf(cmd, sizeof(cmd), "generated_sql/jungle_correctness_success_%s.sql", opt.profile);
    {
        double elapsed = 0.0;
        int ok = 0;
        stage_begin("correctness success");
        ok = run_sql_file(cmd, opt.memtrack, &elapsed, &rc, output, output_size) &&
             rc == 0 &&
             !contains_token(output, "[error]") &&
             !contains_token(output, "[오류]");
        stage_end("correctness success", ok, elapsed);
        if (!ok) {
            correctness_pass = 0;
            snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "correctness success script failed");
        }
    }

    snprintf(cmd, sizeof(cmd), "generated_sql/jungle_correctness_failure_%s.sql", opt.profile);
    {
        double elapsed = 0.0;
        int ok = 0;
        stage_begin("correctness failure");
        ok = run_sql_file(cmd, opt.memtrack, &elapsed, &rc, output, output_size) &&
             rc == 0 &&
             count_token(output, "[error]") >= EXPECTED_FAILURE_ERRORS;
        stage_end("correctness failure", ok, elapsed);
        if (!ok) {
            correctness_pass = 0;
            snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "correctness failure script did not emit expected constraint errors");
        }
    }

    iters = (IterationResult *)calloc((size_t)opt.repeat, sizeof(IterationResult));
    id_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    email_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    phone_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    scan_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    insert_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    update_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));
    delete_vals = (double *)calloc((size_t)opt.repeat, sizeof(double));

    if (!iters || !id_vals || !email_vals || !phone_vals || !scan_vals || !insert_vals || !update_vals || !delete_vals) {
        fprintf(stderr, "[error] memory allocation failed\n");
        return 1;
    }

    if (correctness_pass) {
        for (int i = 0; i < opt.repeat; i++) {
            Throughput bench;
            double elapsed = 0.0;
            int cmd_exit = 0;
            double peak;
            char stage_label[128];

            if (opt.memtrack) {
#if defined(_WIN32)
                snprintf(cmd, sizeof(cmd), "set BENCH_MEMTRACK_REPORT=1&& " SQLPROCESSOR_CMD " --benchmark %d 2>&1", opt.rows);
#else
                snprintf(cmd, sizeof(cmd), "BENCH_MEMTRACK_REPORT=1 " SQLPROCESSOR_CMD " --benchmark %d 2>&1", opt.rows);
#endif
            } else {
                snprintf(cmd, sizeof(cmd), SQLPROCESSOR_CMD " --benchmark %d 2>&1", opt.rows);
            }

            snprintf(stage_label, sizeof(stage_label), "iteration %d/%d benchmark", i + 1, opt.repeat);
            stage_begin(stage_label);
            if (!run_command_capture(cmd, output, output_size, &elapsed, &cmd_exit) || cmd_exit != 0 || !parse_benchmark_output(output, &bench)) {
                stage_end(stage_label, 0, elapsed);
                correctness_pass = 0;
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "benchmark command parse failed");
                break;
            }
            stage_end(stage_label, 1, elapsed);
            peak = parse_memtrack_peak(output);
            if (peak > peak_heap) peak_heap = peak;

            if (!reset_workload_csv_header()) {
                correctness_pass = 0;
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "failed to reset workload csv before iteration");
                break;
            }

            snprintf(cmd, sizeof(cmd), "generated_sql/jungle_insert_%s.sql", opt.profile);
            snprintf(stage_label, sizeof(stage_label), "iteration %d/%d insert", i + 1, opt.repeat);
            stage_begin(stage_label);
            if (!run_sql_file(cmd, opt.memtrack, &elapsed, &cmd_exit, output, output_size) || cmd_exit != 0) {
                stage_end(stage_label, 0, elapsed);
                correctness_pass = 0;
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "insert workload failed");
                break;
            }
            stage_end(stage_label, 1, elapsed);
            iters[i].throughput.insert = elapsed > 0.0 ? (double)opt.rows / elapsed : 0.0;
            peak = parse_memtrack_peak(output);
            if (peak > peak_heap) peak_heap = peak;

            snprintf(cmd, sizeof(cmd), "generated_sql/jungle_update_%s.sql", opt.profile);
            snprintf(stage_label, sizeof(stage_label), "iteration %d/%d update", i + 1, opt.repeat);
            stage_begin(stage_label);
            if (!run_sql_file(cmd, opt.memtrack, &elapsed, &cmd_exit, output, output_size) || cmd_exit != 0) {
                stage_end(stage_label, 0, elapsed);
                correctness_pass = 0;
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "update workload failed");
                break;
            }
            stage_end(stage_label, 1, elapsed);
            iters[i].throughput.update = elapsed > 0.0 ? (double)opt.update_rows / elapsed : 0.0;
            peak = parse_memtrack_peak(output);
            if (peak > peak_heap) peak_heap = peak;

            snprintf(cmd, sizeof(cmd), "generated_sql/jungle_delete_%s.sql", opt.profile);
            snprintf(stage_label, sizeof(stage_label), "iteration %d/%d delete", i + 1, opt.repeat);
            stage_begin(stage_label);
            if (!run_sql_file(cmd, opt.memtrack, &elapsed, &cmd_exit, output, output_size)) {
                stage_end(stage_label, 0, elapsed);
                correctness_pass = 0;
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "delete workload failed to execute");
                break;
            }
            stage_end(stage_label, cmd_exit == 0, elapsed);
            iters[i].throughput.delete_op = elapsed > 0.0 ? (double)opt.delete_rows / elapsed : 0.0;
            iters[i].delete_measured = (cmd_exit == 0 && !contains_token(output, "[error]"));
            if (!iters[i].delete_measured) strcpy(delete_mode, "estimated");
            peak = parse_memtrack_peak(output);
            if (peak > peak_heap) peak_heap = peak;

            iters[i].throughput.id_select = bench.id_select;
            iters[i].throughput.uk_email_select = bench.uk_email_select;
            iters[i].throughput.uk_phone_select = bench.uk_phone_select;
            iters[i].throughput.scan_select = bench.scan_select;
            iters[i].peak_heap_requested_bytes = peak;

            id_vals[i] = iters[i].throughput.id_select;
            email_vals[i] = iters[i].throughput.uk_email_select;
            phone_vals[i] = iters[i].throughput.uk_phone_select;
            scan_vals[i] = iters[i].throughput.scan_select;
            insert_vals[i] = iters[i].throughput.insert;
            update_vals[i] = iters[i].throughput.update;
            delete_vals[i] = iters[i].throughput.delete_op;
        }
    }

    peak_process_bytes = ru_maxrss_bytes();
    live_payload = max_live_payload_bytes(opt.rows);

    n_id = n_uk_email = n_uk_phone = n_uk = n_scan = n_insert = n_update = n_delete = 0.0;
    s_select = s_thru = s_util = 0.0;

    if (correctness_pass) {
        double util_base;
        t_id = median(id_vals, opt.repeat);
        t_email = median(email_vals, opt.repeat);
        t_phone = median(phone_vals, opt.repeat);
        t_scan = median(scan_vals, opt.repeat);
        t_insert = median(insert_vals, opt.repeat);
        t_update = median(update_vals, opt.repeat);
        t_delete = median(delete_vals, opt.repeat);

        n_id = normalize(t_id, REF_ID);
        n_uk_email = normalize(t_email, REF_UK);
        n_uk_phone = normalize(t_phone, REF_UK);
        n_uk = (n_uk_email + n_uk_phone) / 2.0;
        n_scan = normalize(t_scan, REF_SCAN);
        n_insert = normalize(t_insert, REF_INSERT);
        n_update = normalize(t_update, REF_UPDATE);

        if (strcmp(delete_mode, "estimated") == 0) {
            n_delete = DELETE_ESTIMATED_NORMALIZED;
        } else {
            n_delete = normalize(t_delete, REF_DELETE);
        }

        if (opt.memtrack && peak_heap > 0.0) {
            util_base = peak_heap;
            strcpy(util_mode, "memtrack");
            util_ref = REF_UTIL_MEMTRACK;
        } else {
            util_base = peak_process_bytes;
            strcpy(util_mode, "proxy");
            util_ref = REF_UTIL_PROXY;
            if (opt.memtrack && peak_heap <= 0.0) {
                strcpy(delete_mode, "estimated");
                snprintf(fail_reasons[fail_count++], sizeof(fail_reasons[0]), "memtrack requested but no memtrack signal captured; util fallback to proxy");
            }
        }

        util_raw = util_base > 0.0 ? live_payload / util_base : 0.0;
        util_norm = normalize(util_raw, util_ref);

        s_select = 0.60 * n_id + 0.30 * n_uk + 0.10 * n_scan;
        s_thru = 0.60 * s_select + 0.20 * n_insert + 0.15 * n_update + 0.05 * n_delete;
        s_util = util_norm;
        score_total = 100.0 * (0.60 * s_thru + 0.40 * s_util);
    }

    total_elapsed = now_seconds() - total_start;

    {
        FILE *json = fopen("artifacts/bench/report.json", "w");
        FILE *raw = fopen("artifacts/bench/report.raw", "w");
        FILE *md = fopen("artifacts/bench/report.md", "w");
        char git_sha[64] = "unknown";
        char ts[64] = "";
        time_t now = time(NULL);
        struct tm *tm_now = localtime(&now);
#if defined(_WIN32)
        FILE *git_pipe = popen("git rev-parse --short HEAD 2>nul", "r");
#else
        FILE *git_pipe = popen("git rev-parse --short HEAD 2>/dev/null", "r");
#endif

        if (tm_now) strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S%z", tm_now);
        if (git_pipe && fgets(git_sha, sizeof(git_sha), git_pipe)) trim_newline(git_sha);
        if (git_pipe) pclose(git_pipe);

        if (!json || !raw || !md) {
            fprintf(stderr, "[error] failed to open report outputs\n");
            if (json) fclose(json);
            if (raw) fclose(raw);
            if (md) fclose(md);
            return 1;
        }

        fprintf(json, "{\n");
        fprintf(json, "  \"throughput\": {\n");
        fprintf(json, "    \"id_select\": %.6f,\n", t_id);
        fprintf(json, "    \"uk_email_select\": %.6f,\n", t_email);
        fprintf(json, "    \"uk_phone_select\": %.6f,\n", t_phone);
        fprintf(json, "    \"scan_select\": %.6f,\n", t_scan);
        fprintf(json, "    \"insert\": %.6f,\n", t_insert);
        fprintf(json, "    \"update\": %.6f,\n", t_update);
        fprintf(json, "    \"delete\": %.6f\n", t_delete);
        fprintf(json, "  },\n");
        fprintf(json, "  \"util\": {\n");
        fprintf(json, "    \"raw\": %.6f,\n", util_raw);
        fprintf(json, "    \"reference\": %.6f,\n", util_ref);
        fprintf(json, "    \"normalized\": %.6f\n", util_norm);
        fprintf(json, "  },\n");
        fprintf(json, "  \"normalized\": {\n");
        fprintf(json, "    \"id\": %.6f,\n", n_id);
        fprintf(json, "    \"uk_email\": %.6f,\n", n_uk_email);
        fprintf(json, "    \"uk_phone\": %.6f,\n", n_uk_phone);
        fprintf(json, "    \"uk\": %.6f,\n", n_uk);
        fprintf(json, "    \"scan\": %.6f,\n", n_scan);
        fprintf(json, "    \"insert\": %.6f,\n", n_insert);
        fprintf(json, "    \"update\": %.6f,\n", n_update);
        fprintf(json, "    \"delete\": %.6f\n", n_delete);
        fprintf(json, "  },\n");
        fprintf(json, "  \"subscores\": {\n");
        fprintf(json, "    \"select\": %.6f,\n", s_select);
        fprintf(json, "    \"throughput\": %.6f,\n", s_thru);
        fprintf(json, "    \"util\": %.6f\n", s_util);
        fprintf(json, "  },\n");
        if (correctness_pass) fprintf(json, "  \"score_total\": %.6f,\n", score_total);
        else fprintf(json, "  \"score_total\": 0,\n");
        fprintf(json, "  \"correctness_pass\": %s,\n", correctness_pass ? "true" : "false");
        fprintf(json, "  \"fail_reasons\": [");
        for (int i = 0; i < fail_count; i++) {
            fprintf(json, "%s\"%s\"", i == 0 ? "" : ", ", fail_reasons[i]);
        }
        fprintf(json, "],\n");
        fprintf(json, "  \"metadata\": {\n");
        fprintf(json, "    \"timestamp\": \"%s\",\n", ts);
        fprintf(json, "    \"seed\": %u,\n", opt.seed);
        fprintf(json, "    \"profile\": \"%s\",\n", opt.profile);
        fprintf(json, "    \"spec_version\": \"%s\",\n", SPEC_VERSION);
        fprintf(json, "    \"git\": \"%s\",\n", git_sha);
        fprintf(json, "    \"repeat\": %d,\n", opt.repeat);
        fprintf(json, "    \"delete_mode\": \"%s\",\n", delete_mode);
        fprintf(json, "    \"util_mode\": \"%s\",\n", util_mode);
        fprintf(json, "    \"peak_process_memory_bytes\": %.0f,\n", peak_process_bytes);
        fprintf(json, "    \"peak_heap_requested_bytes\": %.0f,\n", peak_heap);
        fprintf(json, "    \"max_live_payload_bytes\": %.0f\n", live_payload);
        fprintf(json, "  }\n");
        fprintf(json, "}\n");

        fprintf(raw, "correctness_pass=%s\n", correctness_pass ? "true" : "false");
        fprintf(raw, "delete_mode=%s\n", delete_mode);
        fprintf(raw, "score_total=%s\n", correctness_pass ? "set" : "zero");
        if (correctness_pass) fprintf(raw, "score_value=%.6f\n", score_total);
        else fprintf(raw, "score_value=0\n");
        fprintf(raw, "profile=%s\n", opt.profile);
        fprintf(raw, "seed=%u\n", opt.seed);

        fprintf(md, "# Benchmark Report\n\n");
        fprintf(md, "- correctness_pass: `%s`\n", correctness_pass ? "true" : "false");
        fprintf(md, "- score_total: `%s`\n", correctness_pass ? "calculated" : "0 (correctness fail)");
        fprintf(md, "- score_value: `%.6f`\n", correctness_pass ? score_total : 0.0);
        fprintf(md, "- delete_mode: `%s`\n", delete_mode);
        fprintf(md, "- util_mode: `%s`\n", util_mode);
        fprintf(md, "\n## Throughput (ops/sec)\n\n");
        fprintf(md, "| metric | value |\n");
        fprintf(md, "|---|---:|\n");
        fprintf(md, "| id_select | %.2f |\n", t_id);
        fprintf(md, "| uk_email_select | %.2f |\n", t_email);
        fprintf(md, "| uk_phone_select | %.2f |\n", t_phone);
        fprintf(md, "| scan_select | %.2f |\n", t_scan);
        fprintf(md, "| insert | %.2f |\n", t_insert);
        fprintf(md, "| update | %.2f |\n", t_update);
        fprintf(md, "| delete | %.2f |\n", t_delete);

        fclose(json);
        fclose(raw);
        fclose(md);
    }

    {
        double score_display = correctness_pass ? score_total : 0.0;
        double score_percent = score_display;
        double throughput_score = s_thru * 60.0;
        double util_score = s_util * 40.0;
        double kops_total = (t_insert + t_update + t_delete) / 3000.0;
        double kops_sid = t_id / 1000.0;
        double kops_suk = t_email / 1000.0;
        double kops_sscan = t_scan / 1000.0;
        double kops_ins = t_insert / 1000.0;
        double kops_upd = t_update / 1000.0;
        double kops_del = t_delete / 1000.0;

        if (score_percent > 100.0) score_percent = 100.0;
        if (throughput_score > 60.0) throughput_score = 60.0;
        if (util_score > 40.0) util_score = 40.0;

        printf("[ok] wrote artifacts/bench/report.json\n");
        printf("[ok] wrote artifacts/bench/report.md\n");
        {
            char v_score[32], v_thru[32], v_util[32], v_kops[32];
            char v_sid[32], v_suk[32], v_sscan[32], v_ins[32], v_upd[32], v_del[32];
            snprintf(v_score, sizeof(v_score), "%.2f", score_percent);
            snprintf(v_thru, sizeof(v_thru), "%.2f", throughput_score);
            snprintf(v_util, sizeof(v_util), "%.2f", util_score);
            snprintf(v_kops, sizeof(v_kops), "%.2f", kops_total);
            snprintf(v_sid, sizeof(v_sid), "%.2f", kops_sid);
            snprintf(v_suk, sizeof(v_suk), "%.2f", kops_suk);
            snprintf(v_sscan, sizeof(v_sscan), "%.2f", kops_sscan);
            snprintf(v_ins, sizeof(v_ins), "%.2f", kops_ins);
            snprintf(v_upd, sizeof(v_upd), "%.2f", kops_upd);
            snprintf(v_del, sizeof(v_del), "%.2f", kops_del);

            printf("\n+----------------------+----------------+----------+\n");
            printf("| %-20s | %14s | %-8s |\n", "Metric", "Value", "Unit");
            printf("+----------------------+----------------+----------+\n");
            printf("| %-20s | %14s | %-8s |\n", "Score", v_score, "/100");
            printf("| %-20s | %14s | %-8s |\n", "Correctness Pass", correctness_pass ? "true" : "false", "bool");
            printf("| %-20s | %14s | %-8s |\n", "Throughput Score", v_thru, "/60");
            printf("| %-20s | %14s | %-8s |\n", "Util Score", v_util, "/40");
            printf("| %-20s | %14s | %-8s |\n", "Total CRUD kops", v_kops, "kops");
            printf("| %-20s | %14s | %-8s |\n", "SELECT(id)", v_sid, "kops");
            printf("| %-20s | %14s | %-8s |\n", "SELECT(uk)", v_suk, "kops");
            printf("| %-20s | %14s | %-8s |\n", "SELECT(scan)", v_sscan, "kops");
            printf("| %-20s | %14s | %-8s |\n", "INSERT", v_ins, "kops");
            printf("| %-20s | %14s | %-8s |\n", "UPDATE", v_upd, "kops");
            printf("| %-20s | %14s | %-8s |\n", "DELETE", v_del, "kops");
            printf("| %-20s | %14.2f | %-8s |\n", "Total Time", total_elapsed, "sec");
            printf("+----------------------+----------------+----------+\n");
        }
        printf("profile=%s seed=%u repeat=%d spec=%s\n", opt.profile, opt.seed, opt.repeat, SPEC_VERSION);

        if (fail_count > 0) {
            printf("fail_reasons:\n");
            for (int i = 0; i < fail_count; i++) {
                printf("  - %s\n", fail_reasons[i]);
            }
        }
    }

    free(iters);
    free(id_vals);
    free(email_vals);
    free(phone_vals);
    free(scan_vals);
    free(insert_vals);
    free(update_vals);
    free(delete_vals);
    free(output);

    return 0;
}
