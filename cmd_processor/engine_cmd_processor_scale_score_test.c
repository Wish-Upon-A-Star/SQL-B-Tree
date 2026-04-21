#include "cmd_processor.h"
#include "engine_cmd_processor.h"
#include "engine_cmd_processor_test_support.h"

#include "../executor.h"
#include "../jungle_benchmark.h"
#include "../parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[fail] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

#define SCALE_COUNT 3
#define PING_SAMPLES 1000
#define LIFECYCLE_SAMPLES 1000
#define BENCH_WORKERS 4
#define BENCH_SHARDS 1
#define BENCH_QUEUE_CAPACITY 64
#define BENCH_PLANNER_CACHE 128
#define BENCH_REQUEST_SLOTS 72
#define BENCH_SQL_LIMIT 256
#define BENCH_RESPONSE_CAPACITY 96
#define DATASET_FILE "cmdp_scale_users.csv"
#define DATASET_TABLE "cmdp_scale_users"

static const int kRowCounts[SCALE_COUNT] = {10000, 100000, 1000000};
static const int kRequestCounts[SCALE_COUNT] = {10000, 100000, 1000000};

typedef struct {
    int rows;
    int requests;
    double direct_avg_us;
    double direct_p95_us;
    double processor_avg_us;
    double processor_p95_us;
    double processor_throughput_rps;
    double avg_queue_wait_us;
    double avg_lock_wait_us;
    double avg_exec_us;
    double overhead_score;
    double queue_score;
} ScaleCaseResult;

typedef struct {
    CmdProcessor *processor;
    int rows;
    int request_count;
    int worker_index;
    int failures;
} ConcurrentArg;

typedef struct {
    double lifecycle_score;
    double ping_score;
    double select_matrix_score;
    double queue_matrix_score;
    double memory_score;
    double metrics_score;
    double total_score;
} ScoreBreakdown;

static double clamp01(double value) {
    if (value < 0.0) return 0.0;
    if (value > 1.0) return 1.0;
    return value;
}

static double normalize_lower_better(double actual, double reference) {
    if (actual <= 0.0) return 1.0;
    if (reference <= 0.0) return 0.0;
    return clamp01(reference / actual);
}

static double normalize_higher_better(double actual, double reference) {
    if (reference <= 0.0) return 0.0;
    return clamp01(actual / reference);
}

static void cleanup_dataset(void) {
    close_all_tables();
    remove(DATASET_FILE);
    remove(DATASET_TABLE ".delta");
    remove(DATASET_TABLE ".idx");
}

static int dataset_exists(void) {
    FILE *in = fopen(DATASET_FILE, "rb");
    if (!in) return 0;
    fclose(in);
    return 1;
}

static int prepare_dataset(int rows) {
    cleanup_dataset();
    generate_jungle_dataset(rows, DATASET_FILE);
    return dataset_exists();
}

static int create_processor(CmdProcessor **out_processor) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;

    if (out_processor) *out_processor = NULL;
    if (!out_processor) return -1;

    memset(&context, 0, sizeof(context));
    context.name = "engine_cmd_processor_scale_score";
    context.max_sql_len = BENCH_SQL_LIMIT;
    context.request_buffer_count = BENCH_REQUEST_SLOTS;
    context.response_body_capacity = BENCH_RESPONSE_CAPACITY;

    memset(&options, 0, sizeof(options));
    options.worker_count = BENCH_WORKERS;
    options.shard_count = BENCH_SHARDS;
    options.queue_capacity_per_shard = BENCH_QUEUE_CAPACITY;
    options.planner_cache_capacity = BENCH_PLANNER_CACHE;

    return engine_cmd_processor_create(&context, &options, out_processor);
}

static void build_select_sql(int rows, int request_index, char *sql, size_t sql_size) {
    int id = (request_index % rows) + 1;
    snprintf(sql, sql_size, "SELECT * FROM " DATASET_TABLE " WHERE id = %d", id);
}

static int run_ping(CmdProcessor *processor, const char *request_id, unsigned long long *elapsed_us) {
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;

    if (elapsed_us) *elapsed_us = 0;
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_ping_request(processor, request, request_id) == CMD_STATUS_OK);
    CHECK(engine_cmd_submit_and_wait(processor, request, &response, elapsed_us) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->status == CMD_STATUS_OK);
    CHECK(response->body != NULL && strcmp(response->body, "pong") == 0);
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    return 0;
}

static int run_processor_sql(CmdProcessor *processor,
                             const char *request_id,
                             const char *sql,
                             unsigned long long *elapsed_us) {
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;

    if (elapsed_us) *elapsed_us = 0;
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, request_id, sql) == CMD_STATUS_OK);
    CHECK(engine_cmd_submit_and_wait(processor, request, &response, elapsed_us) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->status == CMD_STATUS_OK);
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    return 0;
}

static int run_direct_sql(const char *sql, unsigned long long *elapsed_us) {
    Statement stmt;
    int matched_rows = 0;
    int affected_rows = 0;
    long generated_id = 0;
    unsigned long long start_us;

    if (elapsed_us) *elapsed_us = 0;
    start_us = engine_cmd_now_us();
    CHECK(parse_statement(sql, &stmt) == 1);
    CHECK(executor_execute_statement(&stmt, &matched_rows, &affected_rows, &generated_id) == 1);
    if (elapsed_us) *elapsed_us = engine_cmd_now_us() - start_us;
    return 0;
}

static int measure_direct_selects(int rows, int requests, double *avg_us_out, double *p95_us_out) {
    unsigned long long *samples;
    unsigned long long total_us = 0;
    int i;

    samples = (unsigned long long *)calloc((size_t)requests, sizeof(*samples));
    CHECK(samples != NULL);
    for (i = 0; i < requests; i++) {
        char sql[96];
        build_select_sql(rows, i, sql, sizeof(sql));
        CHECK(run_direct_sql(sql, &samples[i]) == 0);
        total_us += samples[i];
    }
    *avg_us_out = (double)total_us / (double)requests;
    *p95_us_out = engine_cmd_percentile95_us(samples, requests);
    free(samples);
    return 0;
}

static int measure_processor_selects(CmdProcessor *processor,
                                     int rows,
                                     int requests,
                                     double *avg_us_out,
                                     double *p95_us_out) {
    unsigned long long *samples;
    unsigned long long total_us = 0;
    int i;

    samples = (unsigned long long *)calloc((size_t)requests, sizeof(*samples));
    CHECK(samples != NULL);
    for (i = 0; i < requests; i++) {
        char sql[96];
        char request_id[64];
        build_select_sql(rows, i, sql, sizeof(sql));
        snprintf(request_id, sizeof(request_id), "proc-sel-%d", i);
        CHECK(run_processor_sql(processor, request_id, sql, &samples[i]) == 0);
        total_us += samples[i];
    }
    *avg_us_out = (double)total_us / (double)requests;
    *p95_us_out = engine_cmd_percentile95_us(samples, requests);
    free(samples);
    return 0;
}

static db_thread_return_t DB_THREAD_CALL concurrent_worker(void *arg_ptr) {
    ConcurrentArg *arg = (ConcurrentArg *)arg_ptr;
    int iterations = arg->request_count / BENCH_WORKERS;
    int remainder = arg->request_count % BENCH_WORKERS;
    int count = iterations + (arg->worker_index < remainder ? 1 : 0);
    int offset = arg->worker_index * iterations + (arg->worker_index < remainder ? arg->worker_index : remainder);
    int i;

    for (i = 0; i < count; i++) {
        char sql[96];
        char request_id[64];
        build_select_sql(arg->rows, offset + i, sql, sizeof(sql));
        snprintf(request_id, sizeof(request_id), "conc-%d-%d", arg->worker_index, i);
        if (run_processor_sql(arg->processor, request_id, sql, NULL) != 0) {
            arg->failures = 1;
            break;
        }
    }
#if defined(_WIN32)
    return 0;
#else
    return NULL;
#endif
}

static int measure_processor_concurrent(CmdProcessor *processor,
                                        int rows,
                                        int requests,
                                        double *throughput_rps_out,
                                        double *avg_queue_wait_us_out,
                                        double *avg_lock_wait_us_out,
                                        double *avg_exec_us_out) {
    db_thread_t threads[BENCH_WORKERS];
    ConcurrentArg args[BENCH_WORKERS];
    EngineCmdProcessorStats before_stats;
    EngineCmdProcessorStats after_stats;
    unsigned long long start_us;
    unsigned long long elapsed_us;
    int i;

    memset(&before_stats, 0, sizeof(before_stats));
    memset(&after_stats, 0, sizeof(after_stats));
    CHECK(engine_cmd_processor_snapshot_stats(processor, &before_stats) == 0);
    start_us = engine_cmd_now_us();
    for (i = 0; i < BENCH_WORKERS; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].processor = processor;
        args[i].rows = rows;
        args[i].request_count = requests;
        args[i].worker_index = i;
        CHECK(db_thread_create(&threads[i], concurrent_worker, &args[i]) == 1);
    }
    for (i = 0; i < BENCH_WORKERS; i++) db_thread_join(threads[i]);
    elapsed_us = engine_cmd_now_us() - start_us;
    CHECK(engine_cmd_processor_snapshot_stats(processor, &after_stats) == 0);
    for (i = 0; i < BENCH_WORKERS; i++) CHECK(args[i].failures == 0);

    *throughput_rps_out = elapsed_us > 0 ? ((double)requests * 1000000.0) / (double)elapsed_us : 0.0;
    {
        unsigned long long request_delta = after_stats.total_requests - before_stats.total_requests;
        unsigned long long queue_delta = after_stats.total_queue_wait_us - before_stats.total_queue_wait_us;
        unsigned long long lock_delta = after_stats.total_lock_wait_us - before_stats.total_lock_wait_us;
        unsigned long long exec_delta = after_stats.total_exec_us - before_stats.total_exec_us;
        if (request_delta == 0) request_delta = 1;
        *avg_queue_wait_us_out = (double)queue_delta / (double)request_delta;
        *avg_lock_wait_us_out = (double)lock_delta / (double)request_delta;
        *avg_exec_us_out = (double)exec_delta / (double)request_delta;
    }
    return 0;
}

static double score_overhead(double direct_avg_us,
                             double direct_p95_us,
                             double processor_avg_us,
                             double processor_p95_us) {
    double avg_component = normalize_lower_better(processor_avg_us, direct_avg_us * 2.0 + 0.25);
    double p95_component = normalize_lower_better(processor_p95_us, direct_p95_us * 2.0 + 1.0);
    return 5.0 * ((avg_component * 0.6) + (p95_component * 0.4));
}

static double score_queue(double throughput_rps,
                          double avg_queue_wait_us,
                          double avg_lock_wait_us,
                          double avg_exec_us) {
    double throughput_component = normalize_higher_better(throughput_rps, 180000.0);
    double queue_component = normalize_lower_better(avg_queue_wait_us, 8.0);
    double lock_component = normalize_lower_better(avg_lock_wait_us, 0.20);
    double exec_component = normalize_lower_better(avg_exec_us, 5.0);
    return 5.0 * (throughput_component * 0.4 +
                  queue_component * 0.3 +
                  lock_component * 0.15 +
                  exec_component * 0.15);
}

static int run_scale_case(CmdProcessor *processor,
                          int rows,
                          int requests,
                          ScaleCaseResult *out) {
    memset(out, 0, sizeof(*out));
    out->rows = rows;
    out->requests = requests;
    CHECK(measure_direct_selects(rows, requests, &out->direct_avg_us, &out->direct_p95_us) == 0);
    CHECK(measure_processor_selects(processor, rows, requests, &out->processor_avg_us, &out->processor_p95_us) == 0);
    CHECK(measure_processor_concurrent(processor,
                                       rows,
                                       requests,
                                       &out->processor_throughput_rps,
                                       &out->avg_queue_wait_us,
                                       &out->avg_lock_wait_us,
                                       &out->avg_exec_us) == 0);
    out->overhead_score = score_overhead(out->direct_avg_us,
                                         out->direct_p95_us,
                                         out->processor_avg_us,
                                         out->processor_p95_us);
    out->queue_score = score_queue(out->processor_throughput_rps,
                                   out->avg_queue_wait_us,
                                   out->avg_lock_wait_us,
                                   out->avg_exec_us);
    printf("scale.rows=%d requests=%d direct_avg_us=%.2f direct_p95_us=%.2f processor_avg_us=%.2f processor_p95_us=%.2f throughput_rps=%.2f avg_queue_wait_us=%.2f avg_lock_wait_us=%.2f avg_exec_us=%.2f score.overhead=%.2f/5.00 score.queue=%.2f/5.00\n",
           out->rows, out->requests, out->direct_avg_us, out->direct_p95_us,
           out->processor_avg_us, out->processor_p95_us, out->processor_throughput_rps,
           out->avg_queue_wait_us, out->avg_lock_wait_us, out->avg_exec_us,
           out->overhead_score, out->queue_score);
    return 0;
}

static int measure_lifecycle(CmdProcessor *processor, double *avg_us_out, double *p95_us_out) {
    unsigned long long samples[LIFECYCLE_SAMPLES];
    int i;
    for (i = 0; i < LIFECYCLE_SAMPLES; i++) {
        CmdRequest *request = NULL;
        unsigned long long start_us = engine_cmd_now_us();
        CHECK(cmd_processor_acquire_request(processor, &request) == 0);
        cmd_processor_release_request(processor, request);
        samples[i] = engine_cmd_now_us() - start_us;
    }
    {
        unsigned long long total = 0;
        for (i = 0; i < LIFECYCLE_SAMPLES; i++) total += samples[i];
        *avg_us_out = (double)total / (double)LIFECYCLE_SAMPLES;
        *p95_us_out = engine_cmd_percentile95_us(samples, LIFECYCLE_SAMPLES);
    }
    return 0;
}

static int measure_ping(CmdProcessor *processor, double *avg_us_out, double *p95_us_out) {
    unsigned long long samples[PING_SAMPLES];
    int i;
    for (i = 0; i < PING_SAMPLES; i++) {
        char request_id[64];
        snprintf(request_id, sizeof(request_id), "ping-%d", i);
        CHECK(run_ping(processor, request_id, &samples[i]) == 0);
    }
    {
        unsigned long long total = 0;
        for (i = 0; i < PING_SAMPLES; i++) total += samples[i];
        *avg_us_out = (double)total / (double)PING_SAMPLES;
        *p95_us_out = engine_cmd_percentile95_us(samples, PING_SAMPLES);
    }
    return 0;
}

static ScoreBreakdown score_results(const ScaleCaseResult results[SCALE_COUNT][SCALE_COUNT],
                                    const EngineCmdProcessorStats *stats,
                                    double lifecycle_avg_us,
                                    double lifecycle_p95_us,
                                    double ping_avg_us,
                                    double ping_p95_us) {
    ScoreBreakdown score;
    double overhead_sum = 0.0;
    double queue_sum = 0.0;
    int i;
    int j;

    memset(&score, 0, sizeof(score));
    for (i = 0; i < SCALE_COUNT; i++) {
        for (j = 0; j < SCALE_COUNT; j++) {
            overhead_sum += results[i][j].overhead_score;
            queue_sum += results[i][j].queue_score;
        }
    }

    score.lifecycle_score = 10.0 * (normalize_lower_better(lifecycle_avg_us, 0.30) * 0.6 +
                                    normalize_lower_better(lifecycle_p95_us, 1.50) * 0.4);
    score.ping_score = 10.0 * (normalize_lower_better(ping_avg_us, 0.40) * 0.6 +
                               normalize_lower_better(ping_p95_us, 1.50) * 0.4);
    score.select_matrix_score = 40.0 * clamp01(overhead_sum / 45.0);
    score.queue_matrix_score = 15.0 * clamp01(queue_sum / 45.0);
    score.memory_score = 15.0 * (normalize_lower_better((double)stats->reserved_bytes / 1024.0, 640.0) * 0.6 +
                                 normalize_lower_better((double)stats->peak_jobs_allocated, 80.0) * 0.2 +
                                 normalize_lower_better((double)stats->peak_jobs_in_use, 8.0) * 0.2);
    score.metrics_score = 10.0 * (normalize_lower_better((double)stats->max_queue_depth, 8.0) * 0.4 +
                                  normalize_lower_better((double)stats->max_queue_wait_us, 20.0) * 0.3 +
                                  normalize_lower_better((double)stats->max_exec_us, 20.0) * 0.3);
    score.total_score = score.lifecycle_score +
                        score.ping_score +
                        score.select_matrix_score +
                        score.queue_matrix_score +
                        score.memory_score +
                        score.metrics_score;
    return score;
}

int main(void) {
    CmdProcessor *processor = NULL;
    ScaleCaseResult results[SCALE_COUNT][SCALE_COUNT];
    EngineCmdProcessorStats stats;
    ScoreBreakdown score;
    double lifecycle_avg_us = 0.0;
    double lifecycle_p95_us = 0.0;
    double ping_avg_us = 0.0;
    double ping_p95_us = 0.0;
    int i;
    int j;

    memset(results, 0, sizeof(results));
    memset(&stats, 0, sizeof(stats));
    set_executor_quiet(1);

    CHECK(create_processor(&processor) == 0);
    CHECK(processor != NULL);
    CHECK(measure_lifecycle(processor, &lifecycle_avg_us, &lifecycle_p95_us) == 0);
    CHECK(measure_ping(processor, &ping_avg_us, &ping_p95_us) == 0);

    printf("metric.avg_lifecycle_us=%.2f\n", lifecycle_avg_us);
    printf("metric.p95_lifecycle_us=%.2f\n", lifecycle_p95_us);
    printf("metric.avg_ping_us=%.2f\n", ping_avg_us);
    printf("metric.p95_ping_us=%.2f\n", ping_p95_us);

    for (i = 0; i < SCALE_COUNT; i++) {
        CHECK(prepare_dataset(kRowCounts[i]) == 1);
        printf("[ok] jungle applicant dataset generated: %s (%d rows)\n", DATASET_FILE, kRowCounts[i]);
        for (j = 0; j < SCALE_COUNT; j++) {
            CHECK(run_scale_case(processor, kRowCounts[i], kRequestCounts[j], &results[i][j]) == 0);
        }
    }

    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    score = score_results(results, &stats, lifecycle_avg_us, lifecycle_p95_us, ping_avg_us, ping_p95_us);

    printf("memory.reserved_kib=%.2f\n", (double)stats.reserved_bytes / 1024.0);
    printf("memory.peak_jobs_allocated=%llu\n", stats.peak_jobs_allocated);
    printf("memory.peak_jobs_in_use=%llu\n", stats.peak_jobs_in_use);
    printf("memory.peak_request_slots_in_use=%llu\n", stats.peak_request_slots_in_use);
    printf("memory.peak_response_slots_in_use=%llu\n", stats.peak_response_slots_in_use);
    if (stats.total_requests > 0) {
        printf("metric.avg_queue_wait_us=%.2f\n", (double)stats.total_queue_wait_us / (double)stats.total_requests);
        printf("metric.avg_lock_wait_us=%.2f\n", (double)stats.total_lock_wait_us / (double)stats.total_requests);
        printf("metric.avg_exec_us=%.2f\n", (double)stats.total_exec_us / (double)stats.total_requests);
    } else {
        printf("metric.avg_queue_wait_us=0.00\n");
        printf("metric.avg_lock_wait_us=0.00\n");
        printf("metric.avg_exec_us=0.00\n");
    }
    printf("metric.max_queue_depth=%d\n", stats.max_queue_depth);
    printf("score.lifecycle=%.2f/10.00\n", score.lifecycle_score);
    printf("score.ping=%.2f/10.00\n", score.ping_score);
    printf("score.select_matrix=%.2f/40.00\n", score.select_matrix_score);
    printf("score.queue_matrix=%.2f/15.00\n", score.queue_matrix_score);
    printf("score.memory=%.2f/15.00\n", score.memory_score);
    printf("score.metrics=%.2f/10.00\n", score.metrics_score);
    printf("score.total=%.2f/100.00\n", score.total_score);

    cmd_processor_shutdown(processor);
    cleanup_dataset();
    set_executor_quiet(0);
    return 0;
}
