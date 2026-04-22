#include "cmd_processor.h"
#include "engine_cmd_processor.h"
#include "engine_cmd_processor_test_support.h"

#include "../executor.h"
#include "../jungle_benchmark.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[fail] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

#define TEST_TABLE_A "cmdp_mt_users_a"
#define TEST_TABLE_B "cmdp_mt_users_b"
#define TEST_FILE_A TEST_TABLE_A ".csv"
#define TEST_FILE_B TEST_TABLE_B ".csv"

typedef struct {
    CmdProcessor *processor;
    const char *request_id;
    const char *sql;
    int failed;
    int expect_ok;
    int observed_ok;
    CmdStatusCode observed_status;
    db_mutex_t *start_mutex;
    db_cond_t *start_cond;
    int *start_flag;
} SubmitArg;

static unsigned long test_hash_text(const char *text) {
    unsigned long hash = 5381UL;
    while (text && *text) {
        hash = ((hash << 5) + hash) + (unsigned char)*text;
        text++;
    }
    return hash;
}

typedef struct {
    const char *table_name;
    const char *csv_file;
} TestTableSpec;

static const TestTableSpec g_table_specs[] = {
    {TEST_TABLE_A, TEST_FILE_A},
    {TEST_TABLE_B, TEST_FILE_B}
};

static void cleanup_tables(void) {
    close_all_tables();
    remove(TEST_FILE_A);
    remove(TEST_FILE_B);
    remove(TEST_TABLE_A ".delta");
    remove(TEST_TABLE_B ".delta");
    remove(TEST_TABLE_A ".idx");
    remove(TEST_TABLE_B ".idx");
}

static int file_exists(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t row_count;
    uint16_t column_count;
    uint16_t reserved;
} BinaryRowsetHeader;

static uint16_t read_u16_le(const unsigned char *ptr) {
    return (uint16_t)((uint16_t)ptr[0] | ((uint16_t)ptr[1] << 8));
}

static uint32_t read_u32_le(const unsigned char *ptr) {
    return (uint32_t)((uint32_t)ptr[0] |
                      ((uint32_t)ptr[1] << 8) |
                      ((uint32_t)ptr[2] << 16) |
                      ((uint32_t)ptr[3] << 24));
}

static int parse_binary_rowset_header(const CmdResponse *response,
                                      BinaryRowsetHeader *out_header,
                                      size_t *out_offset) {
    const unsigned char *bytes;

    if (!response || !out_header || !out_offset) return 0;
    if (!response->body || response->body_len < 16) return 0;
    bytes = (const unsigned char *)response->body;
    out_header->magic = read_u32_le(bytes);
    out_header->version = read_u16_le(bytes + 4);
    out_header->flags = read_u16_le(bytes + 6);
    out_header->row_count = read_u32_le(bytes + 8);
    out_header->column_count = read_u16_le(bytes + 12);
    out_header->reserved = read_u16_le(bytes + 14);
    *out_offset = 16;
    return 1;
}

static const unsigned char *parse_binary_column_name(const CmdResponse *response,
                                                     const unsigned char *cursor,
                                                     char *out_name,
                                                     size_t out_name_size) {
    uint16_t name_len;

    if (!response || !cursor || !out_name || out_name_size == 0) return NULL;
    if ((size_t)(cursor - (const unsigned char *)response->body) + 2 > response->body_len) return NULL;
    name_len = read_u16_le(cursor);
    cursor += 2;
    if ((size_t)(cursor - (const unsigned char *)response->body) + name_len > response->body_len) return NULL;
    if ((size_t)name_len >= out_name_size) return NULL;
    memcpy(out_name, cursor, name_len);
    out_name[name_len] = '\0';
    return cursor + name_len;
}

static const unsigned char *parse_binary_field(const CmdResponse *response,
                                               const unsigned char *cursor,
                                               char *out_value,
                                               size_t out_value_size) {
    uint32_t field_len;

    if (!response || !cursor || !out_value || out_value_size == 0) return NULL;
    if ((size_t)(cursor - (const unsigned char *)response->body) + 4 > response->body_len) return NULL;
    field_len = read_u32_le(cursor);
    cursor += 4;
    if ((size_t)(cursor - (const unsigned char *)response->body) + field_len > response->body_len) return NULL;
    if ((size_t)field_len >= out_value_size) return NULL;
    memcpy(out_value, cursor, field_len);
    out_value[field_len] = '\0';
    return cursor + field_len;
}

static int create_processor(CmdProcessor **out_processor) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;

    if (out_processor) *out_processor = NULL;
    memset(&context, 0, sizeof(context));
    context.name = "engine_cmd_processor_test";
    context.max_sql_len = 256;
    context.request_buffer_count = 16;
    context.response_body_capacity = 256;

    memset(&options, 0, sizeof(options));
    options.worker_count = 2;
    options.shard_count = 1;
    options.queue_capacity_per_shard = 16;
    options.planner_cache_capacity = 32;
    return engine_cmd_processor_create(&context, &options, out_processor);
}

static int create_processor_with_body_capacity(size_t response_body_capacity,
                                              int shard_count,
                                              CmdProcessor **out_processor) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;

    if (out_processor) *out_processor = NULL;
    memset(&context, 0, sizeof(context));
    context.name = "engine_cmd_processor_test";
    context.max_sql_len = 256;
    context.request_buffer_count = 16;
    context.response_body_capacity = response_body_capacity;

    memset(&options, 0, sizeof(options));
    options.worker_count = 2;
    options.shard_count = shard_count;
    options.queue_capacity_per_shard = 16;
    options.planner_cache_capacity = 32;
    return engine_cmd_processor_create(&context, &options, out_processor);
}

static int submit_sql(CmdProcessor *processor,
                      const char *request_id,
                      const char *sql,
                      CmdResponse **out_response) {
    CmdRequest *request = NULL;

    if (out_response) *out_response = NULL;
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, request_id, sql) == CMD_STATUS_OK);
    CHECK(engine_cmd_submit_and_wait(processor, request, out_response, NULL) == 0);
    cmd_processor_release_request(processor, request);
    return 0;
}

static int run_sql(CmdProcessor *processor, const char *request_id, const char *sql) {
    CmdResponse *response = NULL;

    CHECK(submit_sql(processor, request_id, sql, &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    cmd_processor_release_response(processor, response);
    return 0;
}

static db_thread_return_t DB_THREAD_CALL submit_worker(void *arg_ptr) {
    SubmitArg *arg = (SubmitArg *)arg_ptr;
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;
    if (arg->start_mutex && arg->start_cond && arg->start_flag) {
        db_mutex_lock(arg->start_mutex);
        while (!*arg->start_flag) db_cond_wait(arg->start_cond, arg->start_mutex);
        db_mutex_unlock(arg->start_mutex);
    }
    if (cmd_processor_acquire_request(arg->processor, &request) != 0) {
        arg->failed = 1;
    } else if (cmd_processor_set_sql_request(arg->processor, request, arg->request_id, arg->sql) != CMD_STATUS_OK) {
        arg->failed = 1;
        cmd_processor_release_request(arg->processor, request);
        request = NULL;
    } else if (engine_cmd_submit_and_wait(arg->processor, request, &response, NULL) != 0 || !response) {
        arg->failed = 1;
    } else {
        arg->observed_ok = response->ok;
        arg->observed_status = response->status;
        if (arg->expect_ok >= 0 && !!response->ok != !!arg->expect_ok) arg->failed = 1;
        cmd_processor_release_response(arg->processor, response);
    }
    if (request) cmd_processor_release_request(arg->processor, request);
#if defined(_WIN32)
    return 0;
#else
    return NULL;
#endif
}

static int create_test_tables(void) {
    size_t i;

    for (i = 0; i < sizeof(g_table_specs) / sizeof(g_table_specs[0]); i++) {
        generate_jungle_dataset(16, g_table_specs[i].csv_file);
        CHECK(file_exists(g_table_specs[i].csv_file) == 1);
    }
    return 0;
}

static int test_select_response_contains_rows_binary(void) {
    CmdProcessor *processor = NULL;
    CmdResponse *response = NULL;
    BinaryRowsetHeader header;
    size_t offset = 0;
    const unsigned char *cursor = NULL;
    char first_column[32];
    char second_column[32];
    char first_row_id[32];
    char first_row_email[128];

    cleanup_tables();
    CHECK(create_test_tables() == 0);
    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(4096, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(submit_sql(processor,
                     "select-json",
                     "SELECT id, email FROM " TEST_TABLE_A " WHERE id BETWEEN 1 AND 2",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->body_format == CMD_BODY_BINARY);
    CHECK(response->row_count == 2);
    CHECK(response->body != NULL);
    CHECK(parse_binary_rowset_header(response, &header, &offset) == 1);
    CHECK(header.magic == 0x31524442u);
    CHECK(header.version == 1);
    CHECK(header.row_count == 2);
    CHECK(header.column_count == 2);
    cursor = (const unsigned char *)response->body + offset;
    cursor = parse_binary_column_name(response, cursor, first_column, sizeof(first_column));
    CHECK(cursor != NULL);
    cursor = parse_binary_column_name(response, cursor, second_column, sizeof(second_column));
    CHECK(cursor != NULL);
    CHECK(strcmp(first_column, "id") == 0);
    CHECK(strcmp(second_column, "email") == 0);
    cursor = parse_binary_field(response, cursor, first_row_id, sizeof(first_row_id));
    CHECK(cursor != NULL);
    cursor = parse_binary_field(response, cursor, first_row_email, sizeof(first_row_email));
    CHECK(cursor != NULL);
    CHECK(strcmp(first_row_id, "1") == 0);
    CHECK(strstr(first_row_email, "@") != NULL);

    cmd_processor_release_response(processor, response);
    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_select_response_too_large_reports_error(void) {
    CmdProcessor *processor = NULL;
    CmdResponse *response = NULL;

    cleanup_tables();
    CHECK(create_test_tables() == 0);
    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(96, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(submit_sql(processor,
                     "select-too-large",
                     "SELECT * FROM " TEST_TABLE_A " WHERE id BETWEEN 1 AND 4",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 0);
    CHECK(response->status == CMD_STATUS_PROCESSING_ERROR);
    CHECK(response->error_message != NULL);
    CHECK(strstr(response->error_message, "response too large") != NULL);

    cmd_processor_release_response(processor, response);
    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_execution_across_tables(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];

    cleanup_tables();
    generate_jungle_dataset(120000, TEST_FILE_A);
    generate_jungle_dataset(120000, TEST_FILE_B);
    CHECK(file_exists(TEST_FILE_A) == 1);
    CHECK(file_exists(TEST_FILE_B) == 1);

    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(256, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(run_sql(processor, "warm-a", "SELECT id FROM " TEST_TABLE_A " WHERE name = '__warmup_miss__'") == 0);
    CHECK(run_sql(processor, "warm-b", "SELECT id FROM " TEST_TABLE_B " WHERE name = '__warmup_miss__'") == 0);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "parallel-a";
    args[0].sql = "SELECT id FROM " TEST_TABLE_A " WHERE name = '__parallel_miss__'";
    args[0].expect_ok = -1;
    args[1].processor = processor;
    args[1].request_id = "parallel-b";
    args[1].sql = "SELECT id FROM " TEST_TABLE_B " WHERE name = '__parallel_miss__'";
    args[1].expect_ok = 1;

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);

    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions >= 2);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_execution_same_table_reads(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    char sql_a[256];
    char sql_b[256];
    int suffix_a = 0;
    int suffix_b = 1;

    cleanup_tables();
    generate_jungle_dataset(120000, TEST_FILE_A);
    CHECK(file_exists(TEST_FILE_A) == 1);

    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(256, 2, &processor) == 0);
    CHECK(processor != NULL);

    for (;;) {
        snprintf(sql_a, sizeof(sql_a), "SELECT id FROM " TEST_TABLE_A " WHERE name = '__parallel_same_%d__'", suffix_a);
        snprintf(sql_b, sizeof(sql_b), "SELECT id FROM " TEST_TABLE_A " WHERE name = '__parallel_same_%d__'", suffix_b);
        if ((test_hash_text(sql_a) % 2UL) != (test_hash_text(sql_b) % 2UL)) break;
        suffix_b++;
    }

    CHECK(run_sql(processor, "warm-same-a", sql_a) == 0);
    CHECK(run_sql(processor, "warm-same-b", sql_b) == 0);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "same-a";
    args[0].sql = sql_a;
    args[0].expect_ok = 1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "same-b";
    args[1].sql = sql_b;
    args[1].expect_ok = 1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);
    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions >= 2);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_execution_same_id_reads(void) {
    CmdProcessor *processor = NULL;
    db_thread_t threads[8];
    SubmitArg args[8];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    char sql_a[256];
    char sql_b[256];
    const char *candidates[] = {
        "SELECT email FROM " TEST_TABLE_A " WHERE id = 70000",
        "SELECT name FROM " TEST_TABLE_A " WHERE id = 70000",
        "SELECT id, email FROM " TEST_TABLE_A " WHERE id = 70000",
        "SELECT email FROM " TEST_TABLE_A " WHERE id BETWEEN 70000 AND 70000"
    };
    size_t candidate_index;

    cleanup_tables();
    generate_jungle_dataset(120000, TEST_FILE_A);
    CHECK(file_exists(TEST_FILE_A) == 1);

    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(256, 2, &processor) == 0);
    CHECK(processor != NULL);

    snprintf(sql_a, sizeof(sql_a), "SELECT id FROM " TEST_TABLE_A " WHERE id = 70000");
    sql_b[0] = '\0';
    for (candidate_index = 0; candidate_index < sizeof(candidates) / sizeof(candidates[0]); candidate_index++) {
        if ((test_hash_text(sql_a) % 2UL) != (test_hash_text(candidates[candidate_index]) % 2UL)) {
            strncpy(sql_b, candidates[candidate_index], sizeof(sql_b) - 1);
            sql_b[sizeof(sql_b) - 1] = '\0';
            break;
        }
    }
    CHECK(sql_b[0] != '\0');

    CHECK(run_sql(processor, "warm-same-id-a", sql_a) == 0);
    CHECK(run_sql(processor, "warm-same-id-b", sql_b) == 0);

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "same-id-a0";
    args[0].sql = sql_a;
    args[0].expect_ok = 1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "same-id-b0";
    args[1].sql = sql_b;
    args[1].expect_ok = 1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;
    args[2] = args[0];
    args[2].request_id = "same-id-a1";
    args[3] = args[1];
    args[3].request_id = "same-id-b1";
    args[4] = args[0];
    args[4].request_id = "same-id-a2";
    args[5] = args[1];
    args[5].request_id = "same-id-b2";
    args[6] = args[0];
    args[6].request_id = "same-id-a3";
    args[7] = args[1];
    args[7].request_id = "same-id-b3";

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    CHECK(db_thread_create(&threads[2], submit_worker, &args[2]) == 1);
    CHECK(db_thread_create(&threads[3], submit_worker, &args[3]) == 1);
    CHECK(db_thread_create(&threads[4], submit_worker, &args[4]) == 1);
    CHECK(db_thread_create(&threads[5], submit_worker, &args[5]) == 1);
    CHECK(db_thread_create(&threads[6], submit_worker, &args[6]) == 1);
    CHECK(db_thread_create(&threads[7], submit_worker, &args[7]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_thread_join(threads[2]);
    db_thread_join(threads[3]);
    db_thread_join(threads[4]);
    db_thread_join(threads[5]);
    db_thread_join(threads[6]);
    db_thread_join(threads[7]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(args[2].failed == 0);
    CHECK(args[3].failed == 0);
    CHECK(args[4].failed == 0);
    CHECK(args[5].failed == 0);
    CHECK(args[6].failed == 0);
    CHECK(args[7].failed == 0);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_insert_same_pk_rejects_duplicate(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    CmdResponse *response = NULL;

    cleanup_tables();
    CHECK(create_test_tables() == 0);
    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(4096, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "dup-pk-a";
    args[0].sql = "INSERT INTO " TEST_TABLE_A " VALUES (999001,'dup-pk@test.com','010-9999-0001','pw','DupPkA')";
    args[0].expect_ok = -1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "dup-pk-b";
    args[1].sql = "INSERT INTO " TEST_TABLE_A " VALUES (999001,'dup-pk-other@test.com','010-9999-0002','pw','DupPkB')";
    args[1].expect_ok = -1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK((args[0].observed_ok + args[1].observed_ok) == 1);
    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions == 1);

    CHECK(submit_sql(processor,
                     "verify-dup-pk",
                     "SELECT id, email FROM " TEST_TABLE_A " WHERE id = 999001",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->row_count == 1);
    cmd_processor_release_response(processor, response);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_insert_same_uk_rejects_duplicate(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    CmdResponse *response = NULL;

    cleanup_tables();
    CHECK(create_test_tables() == 0);
    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(4096, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "dup-uk-a";
    args[0].sql = "INSERT INTO " TEST_TABLE_A " VALUES (999011,'dup-uk@test.com','010-9999-0011','pw','DupUkA')";
    args[0].expect_ok = -1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "dup-uk-b";
    args[1].sql = "INSERT INTO " TEST_TABLE_A " VALUES (999012,'dup-uk@test.com','010-9999-0012','pw','DupUkB')";
    args[1].expect_ok = -1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK((args[0].observed_ok + args[1].observed_ok) == 1);
    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions == 1);

    CHECK(submit_sql(processor,
                     "verify-dup-uk",
                     "SELECT id, email FROM " TEST_TABLE_A " WHERE email = 'dup-uk@test.com'",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->row_count == 1);
    cmd_processor_release_response(processor, response);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_same_table_read_write_serializes(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    CmdResponse *response = NULL;

    cleanup_tables();
    generate_jungle_dataset(120000, TEST_FILE_A);
    CHECK(file_exists(TEST_FILE_A) == 1);

    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(4096, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(run_sql(processor, "warm-rw-read", "SELECT id FROM " TEST_TABLE_A " WHERE name = '__rw_guard__'") == 0);
    CHECK(run_sql(processor, "warm-rw-write", "INSERT INTO " TEST_TABLE_A " VALUES (999021,'rw-guard@test.com','010-9999-0021','pw','WarmRw')") == 0);

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "rw-read";
    args[0].sql = "SELECT id FROM " TEST_TABLE_A " WHERE name = '__parallel_rw_guard__'";
    args[0].expect_ok = 1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "rw-write";
    args[1].sql = "INSERT INTO " TEST_TABLE_A " VALUES (999022,'rw-guard-2@test.com','010-9999-0022','pw','ParallelRw')";
    args[1].expect_ok = 1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions == 1);

    CHECK(submit_sql(processor,
                     "verify-rw-insert",
                     "SELECT id, email FROM " TEST_TABLE_A " WHERE id = 999022",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->row_count == 1);
    cmd_processor_release_response(processor, response);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

static int test_parallel_same_id_read_update_serializes(void) {
    CmdProcessor *processor = NULL;
    EngineCmdProcessorStats stats;
    db_thread_t threads[2];
    SubmitArg args[2];
    db_mutex_t start_mutex;
    db_cond_t start_cond;
    int start_flag = 0;
    CmdResponse *response = NULL;
    BinaryRowsetHeader header;
    size_t offset = 0;
    const unsigned char *cursor = NULL;
    char first_column[32];
    char row_value[128];

    cleanup_tables();
    generate_jungle_dataset(120000, TEST_FILE_A);
    CHECK(file_exists(TEST_FILE_A) == 1);

    set_executor_quiet(1);
    CHECK(create_processor_with_body_capacity(4096, 2, &processor) == 0);
    CHECK(processor != NULL);

    CHECK(run_sql(processor, "warm-id-read", "SELECT email FROM " TEST_TABLE_A " WHERE id = 60000") == 0);
    CHECK(run_sql(processor, "warm-id-update", "UPDATE " TEST_TABLE_A " SET name = 'UpdatedConcurrentName' WHERE id = 60000") == 0);

    CHECK(db_mutex_init(&start_mutex) == 1);
    CHECK(db_cond_init(&start_cond) == 1);

    memset(args, 0, sizeof(args));
    args[0].processor = processor;
    args[0].request_id = "same-id-read";
    args[0].sql = "SELECT name FROM " TEST_TABLE_A " WHERE id = 60000";
    args[0].expect_ok = 1;
    args[0].start_mutex = &start_mutex;
    args[0].start_cond = &start_cond;
    args[0].start_flag = &start_flag;
    args[1].processor = processor;
    args[1].request_id = "same-id-update";
    args[1].sql = "UPDATE " TEST_TABLE_A " SET name = 'UpdatedConcurrentName2' WHERE id = 60000";
    args[1].expect_ok = 1;
    args[1].start_mutex = &start_mutex;
    args[1].start_cond = &start_cond;
    args[1].start_flag = &start_flag;

    CHECK(db_thread_create(&threads[0], submit_worker, &args[0]) == 1);
    CHECK(db_thread_create(&threads[1], submit_worker, &args[1]) == 1);
    db_mutex_lock(&start_mutex);
    start_flag = 1;
    db_cond_broadcast(&start_cond);
    db_mutex_unlock(&start_mutex);
    db_thread_join(threads[0]);
    db_thread_join(threads[1]);
    db_cond_destroy(&start_cond);
    db_mutex_destroy(&start_mutex);

    CHECK(args[0].failed == 0);
    CHECK(args[1].failed == 0);
    CHECK(engine_cmd_processor_snapshot_stats(processor, &stats) == 0);
    CHECK(stats.max_concurrent_executions == 1);

    CHECK(submit_sql(processor,
                     "verify-id-update",
                     "SELECT name FROM " TEST_TABLE_A " WHERE id = 60000",
                     &response) == 0);
    CHECK(response != NULL);
    CHECK(response->ok == 1);
    CHECK(response->row_count == 1);
    CHECK(parse_binary_rowset_header(response, &header, &offset) == 1);
    cursor = (const unsigned char *)response->body + offset;
    cursor = parse_binary_column_name(response, cursor, first_column, sizeof(first_column));
    CHECK(cursor != NULL);
    CHECK(strcmp(first_column, "name") == 0);
    cursor = parse_binary_field(response, cursor, row_value, sizeof(row_value));
    CHECK(cursor != NULL);
    CHECK(strcmp(row_value, "UpdatedConcurrentName2") == 0);
    cmd_processor_release_response(processor, response);

    cmd_processor_shutdown(processor);
    cleanup_tables();
    set_executor_quiet(0);
    return 0;
}

int main(void) {
    CHECK(test_select_response_contains_rows_binary() == 0);
    CHECK(test_select_response_too_large_reports_error() == 0);
    CHECK(test_parallel_execution_across_tables() == 0);
    CHECK(test_parallel_execution_same_table_reads() == 0);
    CHECK(test_parallel_execution_same_id_reads() == 0);
    CHECK(test_parallel_insert_same_pk_rejects_duplicate() == 0);
    CHECK(test_parallel_insert_same_uk_rejects_duplicate() == 0);
    CHECK(test_parallel_same_table_read_write_serializes() == 0);
    CHECK(test_parallel_same_id_read_update_serializes() == 0);
    printf("[ok] engine cmd processor parallel execution test passed\n");
    return 0;
}
