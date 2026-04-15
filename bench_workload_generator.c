#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define TABLE_NAME "jungle_workload_users"
#define SOURCE_CSV "jungle_benchmark_users.csv"
#define WORKLOAD_CSV "jungle_workload_users.csv"

#if defined(_WIN32)
#define SQLPROCESSOR_CMD "sqlsprocessor.exe"
#else
#define SQLPROCESSOR_CMD "./sqlsprocessor"
#endif

#define LINE_MAX_LEN 2048
#define SQL_MAX_LEN 8192

typedef struct {
    const char *profile;
    int rows;
    int update_rows;
    int delete_rows;
    int mixed_ops;
    unsigned int seed;
    const char *output_dir;
} GeneratorOptions;

static int is_numeric_col(int col_idx) {
    return col_idx == 0 || col_idx == 7;
}

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

static void trim_newline(char *s) {
    size_t len;
    if (!s) return;
    len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[--len] = '\0';
    }
}

static int file_exists(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static int count_csv_rows(const char *path) {
    FILE *f = fopen(path, "r");
    int lines = 0;
    char buf[LINE_MAX_LEN];
    if (!f) return -1;
    while (fgets(buf, sizeof(buf), f)) lines++;
    fclose(f);
    if (lines <= 0) return 0;
    return lines - 1;
}

static int ensure_source_csv(int rows) {
    char cmd[512];
    int header_ok = 0;
    if (file_exists(SOURCE_CSV) && count_csv_rows(SOURCE_CSV) >= rows) {
        FILE *f = fopen(SOURCE_CSV, "r");
        char line[LINE_MAX_LEN];
        if (f && fgets(line, sizeof(line), f)) {
            trim_newline(line);
            header_ok = strstr(line, "id(PK),email(UK),phone(UK),name,track(NN),") == line;
        }
        if (f) fclose(f);
        if (header_ok) return 1;
    }

    snprintf(cmd, sizeof(cmd), SQLPROCESSOR_CMD " --generate-jungle %d %s", rows, SOURCE_CSV);
    if (system(cmd) != 0) {
        fprintf(stderr, "[error] failed to generate source CSV: %s\n", cmd);
        return 0;
    }
    return 1;
}

static int ensure_dir(const char *path) {
#if defined(_WIN32)
    int rc = mkdir(path);
#else
    int rc = mkdir(path, 0755);
#endif
    if (rc == 0 || errno == EEXIST) return 1;
    fprintf(stderr, "[error] failed to create directory '%s'\n", path);
    return 0;
}

static int parse_csv_fields(const char *line, char fields[][256], int max_fields) {
    int count = 0;
    const char *p = line;
    while (*p && count < max_fields) {
        int idx = 0;
        while (*p && *p != ',' && idx < 255) {
            fields[count][idx++] = *p++;
        }
        fields[count][idx] = '\0';
        count++;
        if (*p == ',') p++;
    }
    return count;
}

static void append_sql_escaped(char *dst, size_t dst_size, size_t *off, const char *src) {
    while (*src && *off + 2 < dst_size) {
        if (*src == '\'') {
            if (*off + 2 >= dst_size) return;
            dst[(*off)++] = '\'';
            dst[(*off)++] = '\'';
            src++;
            continue;
        }
        dst[(*off)++] = *src++;
    }
}

static int csv_row_to_sql_values(const char *line, char *out, size_t out_size) {
    char fields[32][256];
    int field_count = parse_csv_fields(line, fields, 32);
    int i;
    size_t off = 0;

    if (field_count <= 0) return 0;

    for (i = 0; i < field_count; i++) {
        if (i > 0) {
            if (off + 2 >= out_size) return 0;
            out[off++] = ',';
            out[off++] = ' ';
        }

        if (is_numeric_col(i)) {
            size_t len = strlen(fields[i]);
            if (off + len + 1 >= out_size) return 0;
            memcpy(out + off, fields[i], len);
            off += len;
        } else {
            if (off + 2 >= out_size) return 0;
            out[off++] = '\'';
            append_sql_escaped(out, out_size, &off, fields[i]);
            if (off + 1 >= out_size) return 0;
            out[off++] = '\'';
        }
    }

    out[off] = '\0';
    return 1;
}

static void shuffle_ids(int *ids, int n, unsigned int seed) {
    int i;
    srand(seed);
    for (i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = ids[i];
        ids[i] = ids[j];
        ids[j] = tmp;
    }
}

static int write_correctness_sql(const GeneratorOptions *opt) {
    char path_success[512];
    char path_failure[512];
    FILE *f;

    snprintf(path_success, sizeof(path_success), "%s/jungle_correctness_success_%s.sql", opt->output_dir, opt->profile);
    snprintf(path_failure, sizeof(path_failure), "%s/jungle_correctness_failure_%s.sql", opt->output_dir, opt->profile);

    f = fopen(path_success, "w");
    if (!f) return 0;
    fprintf(f,
            "-- correctness success for profile=%s\n"
            "DELETE FROM %s WHERE id = 900000001;\n"
            "DELETE FROM %s WHERE id = 900000002;\n"
            "INSERT INTO %s VALUES (900000001, 'bench_success_1@apply.kr', '010-9000-0001', '성공테스트1', 'sw_ai_lab', 'student', 'major_cs_grade_3', 88, 'gh_bench_success1', 'submitted', '2026_spring');\n"
            "INSERT INTO %s VALUES (900000002, 'bench_success_2@apply.kr', '010-9000-0002', '성공테스트2', 'game_lab', 'student', 'major_game_grade_3', 71, 'gh_bench_success2', 'submitted', '2026_spring');\n"
            "SELECT id, email, phone, name, status FROM %s WHERE id = 900000001;\n"
            "SELECT id, email, phone, name, status FROM %s WHERE email = 'bench_success_2@apply.kr';\n"
            "UPDATE %s SET status = 'pretest_pass' WHERE id = 900000001;\n"
            "DELETE FROM %s WHERE id = 900000002;\n",
            opt->profile,
            TABLE_NAME, TABLE_NAME,
            TABLE_NAME, TABLE_NAME,
            TABLE_NAME, TABLE_NAME,
            TABLE_NAME,
            TABLE_NAME);
    fclose(f);

    f = fopen(path_failure, "w");
    if (!f) return 0;
    fprintf(f,
            "-- correctness failure for profile=%s\n"
            "INSERT INTO %s VALUES (900000001, 'bench_dup_pk@apply.kr', '010-9000-1001', '중복PK', 'sw_ai_lab', 'student', 'major_ai_grade_2', 90, 'gh_dup_pk', 'submitted', '2026_spring');\n"
            "INSERT INTO %s VALUES (900000010, 'bench_success_1@apply.kr', '010-9000-1010', '중복EMAIL', 'sw_ai_lab', 'student', 'major_ai_grade_2', 90, 'gh_dup_email', 'submitted', '2026_spring');\n"
            "INSERT INTO %s VALUES (900000011, 'bench_dup_phone@apply.kr', '010-9000-0001', '중복PHONE', 'sw_ai_lab', 'student', 'major_ai_grade_2', 90, 'gh_dup_phone', 'submitted', '2026_spring');\n"
            "INSERT INTO %s VALUES (900000012, 'bench_nn_fail@apply.kr', '010-9000-1012', 'NN실패', '', 'student', 'major_ai_grade_2', 90, 'gh_nn_fail', 'submitted', '2026_spring');\n",
            opt->profile,
            TABLE_NAME,
            TABLE_NAME,
            TABLE_NAME,
            TABLE_NAME);
    fclose(f);

    return 1;
}

static int write_meta_files(const GeneratorOptions *opt, int generated_rows) {
    char meta_path[512];
    char oracle_path[512];
    FILE *meta_f;
    FILE *oracle_f;
    time_t now = time(NULL);

    snprintf(meta_path, sizeof(meta_path), "%s/workload_%s.meta.json", opt->output_dir, opt->profile);
    snprintf(oracle_path, sizeof(oracle_path), "%s/oracle_%s.json", opt->output_dir, opt->profile);

    meta_f = fopen(meta_path, "w");
    if (!meta_f) return 0;
    fprintf(meta_f,
            "{\n"
            "  \"profile\": \"%s\",\n"
            "  \"seed\": %u,\n"
            "  \"row_count\": %d,\n"
            "  \"update_row_count\": %d,\n"
            "  \"delete_row_count\": %d,\n"
            "  \"preload\": %d,\n"
            "  \"op_count\": %d,\n"
            "  \"crud_ratio\": {\"select\": 60, \"insert\": 20, \"update\": 15, \"delete\": 5},\n"
            "  \"created_at_epoch\": %ld\n"
            "}\n",
            opt->profile, opt->seed, generated_rows, opt->update_rows, opt->delete_rows, opt->rows, opt->mixed_ops, (long)now);
    fclose(meta_f);

    oracle_f = fopen(oracle_path, "w");
    if (!oracle_f) return 0;
    fprintf(oracle_f,
            "{\n"
            "  \"expected_failure_errors_min\": 4,\n"
            "  \"correctness_success_sql\": \"jungle_correctness_success_%s.sql\",\n"
            "  \"correctness_failure_sql\": \"jungle_correctness_failure_%s.sql\"\n"
            "}\n",
            opt->profile, opt->profile);
    fclose(oracle_f);
    return 1;
}

static int generate_sqls(const GeneratorOptions *opt) {
    FILE *src = fopen(SOURCE_CSV, "r");
    FILE *workload = NULL;
    FILE *insert_f = NULL;
    FILE *update_f = NULL;
    FILE *delete_f = NULL;
    char line[LINE_MAX_LEN];
    char values[SQL_MAX_LEN];
    char insert_path[512];
    char update_path[512];
    char delete_path[512];
    char mixed_path[512];
    int *ids = NULL;
    int rows = 0;
    int update_rows = 0;
    int delete_rows = 0;
    FILE *mixed_f = NULL;

    if (!src) {
        fprintf(stderr, "[error] cannot open %s\n", SOURCE_CSV);
        return 0;
    }

    if (!fgets(line, sizeof(line), src)) {
        fclose(src);
        return 0;
    }
    trim_newline(line);

    workload = fopen(WORKLOAD_CSV, "w");
    if (!workload) {
        fclose(src);
        return 0;
    }
    fprintf(workload, "%s\n", line);
    fclose(workload);

    snprintf(insert_path, sizeof(insert_path), "%s/jungle_insert_%s.sql", opt->output_dir, opt->profile);
    snprintf(update_path, sizeof(update_path), "%s/jungle_update_%s.sql", opt->output_dir, opt->profile);
    snprintf(delete_path, sizeof(delete_path), "%s/jungle_delete_%s.sql", opt->output_dir, opt->profile);
    snprintf(mixed_path, sizeof(mixed_path), "%s/workload_%s.sql", opt->output_dir, opt->profile);

    insert_f = fopen(insert_path, "w");
    update_f = fopen(update_path, "w");
    delete_f = fopen(delete_path, "w");
    if (!insert_f || !update_f || !delete_f) {
        if (insert_f) fclose(insert_f);
        if (update_f) fclose(update_f);
        if (delete_f) fclose(delete_f);
        fclose(src);
        return 0;
    }

    ids = (int *)malloc((size_t)opt->rows * sizeof(int));
    if (!ids) {
        fclose(insert_f);
        fclose(update_f);
        fclose(delete_f);
        fclose(src);
        return 0;
    }

    fprintf(insert_f, "-- generated profile=%s rows=%d seed=%u\n", opt->profile, opt->rows, opt->seed);

    while (rows < opt->rows && fgets(line, sizeof(line), src)) {
        char row_copy[LINE_MAX_LEN];
        char *comma;
        trim_newline(line);
        strncpy(row_copy, line, sizeof(row_copy) - 1);
        row_copy[sizeof(row_copy) - 1] = '\0';

        comma = strchr(row_copy, ',');
        if (!comma) continue;
        *comma = '\0';
        ids[rows] = atoi(row_copy);

        if (!csv_row_to_sql_values(line, values, sizeof(values))) {
            fprintf(stderr, "[error] failed to convert row to SQL values\n");
            free(ids);
            fclose(insert_f);
            fclose(update_f);
            fclose(delete_f);
            fclose(src);
            return 0;
        }

        fprintf(insert_f, "INSERT INTO %s VALUES (%s);\n", TABLE_NAME, values);
        rows++;
    }

    update_rows = opt->update_rows <= rows ? opt->update_rows : rows;
    delete_rows = opt->delete_rows <= rows ? opt->delete_rows : rows;
    fprintf(update_f, "-- generated profile=%s rows=%d seed=%u\n", opt->profile, update_rows, opt->seed);
    fprintf(delete_f, "-- generated profile=%s rows=%d seed=%u\n", opt->profile, delete_rows, opt->seed);

    shuffle_ids(ids, rows, opt->seed);
    for (int i = 0; i < update_rows; i++) {
        fprintf(update_f, "UPDATE %s SET status = 'final_wait' WHERE id = %d;\n", TABLE_NAME, ids[i]);
    }
    for (int i = 0; i < delete_rows; i++) {
        fprintf(delete_f, "DELETE FROM %s WHERE id = %d;\n", TABLE_NAME, ids[i]);
    }

    mixed_f = fopen(mixed_path, "w");
    if (!mixed_f) {
        free(ids);
        fclose(insert_f);
        fclose(update_f);
        fclose(delete_f);
        fclose(src);
        return 0;
    }
    srand(opt->seed ^ 0x9e3779b9u);
    fprintf(mixed_f, "-- mixed workload profile=%s ops=%d\n", opt->profile, opt->mixed_ops);
    for (int i = 0; i < opt->mixed_ops && rows > 0; i++) {
        int r = rand() % 100;
        int pick = ids[rand() % rows];
        if (r < 60) {
            fprintf(mixed_f, "SELECT id, email, phone, status FROM %s WHERE id = %d;\n", TABLE_NAME, pick);
        } else if (r < 80) {
            int new_id = 800000000 + i;
            fprintf(mixed_f, "INSERT INTO %s VALUES (%d, 'bench_mix_%d@apply.kr', '010-8000-%04d', '혼합부하', 'sw_ai_lab', 'student', 'mix_generated', 77, 'gh_mix_%d', 'submitted', '2026_spring');\n",
                    TABLE_NAME, new_id, new_id, i % 10000, new_id);
        } else if (r < 95) {
            fprintf(mixed_f, "UPDATE %s SET status = 'pretest_pass' WHERE id = %d;\n", TABLE_NAME, pick);
        } else {
            fprintf(mixed_f, "DELETE FROM %s WHERE id = %d;\n", TABLE_NAME, pick);
        }
    }
    fclose(mixed_f);

    free(ids);
    fclose(insert_f);
    fclose(update_f);
    fclose(delete_f);
    fclose(src);

    if (!write_correctness_sql(opt)) return 0;
    if (!write_meta_files(opt, rows)) return 0;

    printf("[ok] profile=%s rows=%d update_rows=%d delete_rows=%d seed=%u\n",
           opt->profile, rows, update_rows, delete_rows, opt->seed);
    printf("[ok] wrote %s\n", insert_path);
    printf("[ok] wrote %s\n", update_path);
    printf("[ok] wrote %s\n", delete_path);
    printf("[ok] wrote %s\n", mixed_path);
    return 1;
}

static void parse_args(int argc, char **argv, GeneratorOptions *opt) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--profile") == 0 && i + 1 < argc) {
            opt->profile = argv[++i];
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            opt->seed = (unsigned int)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--rows") == 0 && i + 1 < argc) {
            opt->rows = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--preload") == 0 && i + 1 < argc) {
            opt->rows = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--update-rows") == 0 && i + 1 < argc) {
            opt->update_rows = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--delete-rows") == 0 && i + 1 < argc) {
            opt->delete_rows = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--ops") == 0 && i + 1 < argc) {
            opt->mixed_ops = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--output-dir") == 0 && i + 1 < argc) {
            opt->output_dir = argv[++i];
        }
    }

    if (opt->rows <= 0) opt->rows = profile_rows(opt->profile);
    if (opt->update_rows <= 0) opt->update_rows = profile_update_rows(opt->profile);
    if (opt->delete_rows <= 0) opt->delete_rows = profile_delete_rows(opt->profile);
    if (opt->update_rows > opt->rows) opt->update_rows = opt->rows;
    if (opt->delete_rows > opt->rows) opt->delete_rows = opt->rows;
    if (opt->mixed_ops <= 0) opt->mixed_ops = profile_ops(opt->profile);
}

int main(int argc, char **argv) {
    GeneratorOptions opt;

    opt.profile = "score";
    opt.rows = 0;
    opt.update_rows = 0;
    opt.delete_rows = 0;
    opt.mixed_ops = 0;
    opt.seed = 20260415u;
    opt.output_dir = "generated_sql";

    parse_args(argc, argv, &opt);

    if (!ensure_dir(opt.output_dir)) return 1;
    if (!ensure_source_csv(opt.rows)) return 1;
    if (!generate_sqls(&opt)) return 1;

    return 0;
}
