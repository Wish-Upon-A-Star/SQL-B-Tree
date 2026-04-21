#include "cmd_processor.h"
#include "repl_cmd_processor.h"

#include "../executor.h"
#include "../platform_threads.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_TABLE "repl_cmd_processor_test_users"
#define TEST_CSV TEST_TABLE ".csv"
#define TEST_DELTA TEST_TABLE ".delta"
#define TEST_IDX TEST_TABLE ".idx"

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[fail] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

typedef struct {
    int called;
    int release_in_callback;
    char request_id[64];
    CmdStatusCode status;
    int ok;
    CmdBodyFormat body_format;
    char body[8192];
    size_t body_len;
    char error_message[8192];
    int row_count;
    int affected_count;
} CallbackCapture;

typedef struct {
    CmdProcessor *processor;
    int thread_index;
    int failed;
} WorkerArg;

static void cleanup_test_files(void) {
    close_all_tables();
    remove(TEST_CSV);
    remove(TEST_DELTA);
    remove(TEST_IDX);
}

static int write_test_table(void) {
    FILE *file;

    cleanup_test_files();
    file = fopen(TEST_CSV, "w");
    CHECK(file != NULL);
    CHECK(fprintf(file, "id(PK),name\n") > 0);
    CHECK(fclose(file) == 0);
    return 0;
}

static void init_capture(CallbackCapture *capture, int release_in_callback) {
    memset(capture, 0, sizeof(*capture));
    capture->release_in_callback = release_in_callback;
    capture->body_format = CMD_BODY_NONE;
}

static void capture_callback(CmdProcessor *processor,
                             CmdRequest *request,
                             CmdResponse *response,
                             void *user_data) {
    CallbackCapture *capture = (CallbackCapture *)user_data;

    if (!capture) return;
    capture->called++;
    (void)request;

    if (response) {
        snprintf(capture->request_id, sizeof(capture->request_id), "%s", response->request_id);
        capture->status = response->status;
        capture->ok = response->ok;
        capture->body_format = response->body_format;
        capture->body_len = response->body_len;
        capture->row_count = response->row_count;
        capture->affected_count = response->affected_count;
        if (response->body) snprintf(capture->body, sizeof(capture->body), "%s", response->body);
        if (response->error_message) {
            snprintf(capture->error_message,
                     sizeof(capture->error_message),
                     "%s",
                     response->error_message);
        }
    }

    if (capture->release_in_callback) {
        if (response) cmd_processor_release_response(processor, response);
        if (request) cmd_processor_release_request(processor, request);
    }
}

static int submit_ping(CmdProcessor *processor, const char *request_id, CallbackCapture *capture) {
    CmdRequest *request = NULL;

    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_ping_request(processor, request, request_id) == CMD_STATUS_OK);
    CHECK(cmd_processor_submit(processor, request, capture_callback, capture) == 0);
    return 0;
}

static int submit_sql(CmdProcessor *processor,
                      const char *request_id,
                      const char *sql,
                      CallbackCapture *capture) {
    CmdRequest *request = NULL;

    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, request_id, sql) == CMD_STATUS_OK);
    CHECK(cmd_processor_submit(processor, request, capture_callback, capture) == 0);
    return 0;
}

static int test_default_context(void) {
    CmdProcessor *processor = NULL;

    CHECK(repl_cmd_processor_create(NULL, &processor) == 0);
    CHECK(processor != NULL);
    CHECK(processor->context != NULL);
    CHECK(strcmp(processor->context->name, "repl_cmd_processor") == 0);
    CHECK(processor->context->max_sql_len > 0);
    CHECK(processor->context->request_buffer_count == 1);
    CHECK(processor->context->response_body_capacity == 8192);
    CHECK(processor->context->shared_state != NULL);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_context_override(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;

    memset(&context, 0, sizeof(context));
    context.name = "custom_repl";
    context.max_sql_len = 32;
    context.request_buffer_count = 3;
    context.response_body_capacity = 128;

    CHECK(repl_cmd_processor_create(&context, &processor) == 0);
    CHECK(strcmp(processor->context->name, "custom_repl") == 0);
    CHECK(processor->context->max_sql_len == 32);
    CHECK(processor->context->request_buffer_count == 3);
    CHECK(processor->context->response_body_capacity == 128);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_ping(void) {
    CmdProcessor *processor = NULL;
    CallbackCapture capture;

    init_capture(&capture, 1);
    CHECK(repl_cmd_processor_create(NULL, &processor) == 0);
    CHECK(submit_ping(processor, "ping-1", &capture) == 0);
    CHECK(capture.called == 1);
    CHECK(strcmp(capture.request_id, "ping-1") == 0);
    CHECK(capture.status == CMD_STATUS_OK);
    CHECK(capture.ok == 1);
    CHECK(capture.body_format == CMD_BODY_TEXT);
    CHECK(strcmp(capture.body, "pong") == 0);
    CHECK(capture.body_len == 4);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_sql_insert_and_select(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CallbackCapture insert_capture;
    CallbackCapture select_capture;

    CHECK(write_test_table() == 0);
    memset(&context, 0, sizeof(context));
    context.request_buffer_count = 2;
    context.response_body_capacity = 256;

    init_capture(&insert_capture, 1);
    init_capture(&select_capture, 1);
    CHECK(repl_cmd_processor_create(&context, &processor) == 0);

    CHECK(submit_sql(processor,
                     "insert-1",
                     "INSERT INTO " TEST_TABLE " VALUES (1,'kim')",
                     &insert_capture) == 0);
    CHECK(insert_capture.called == 1);
    CHECK(insert_capture.status == CMD_STATUS_OK);
    CHECK(insert_capture.ok == 1);
    CHECK(insert_capture.body_format == CMD_BODY_TEXT);
    CHECK(strstr(insert_capture.body, "INSERT affected_rows=1") != NULL);
    CHECK(insert_capture.affected_count == 1);

    CHECK(submit_sql(processor,
                     "select-1",
                     "SELECT * FROM " TEST_TABLE " WHERE id = 1",
                     &select_capture) == 0);
    CHECK(select_capture.called == 1);
    CHECK(strcmp(select_capture.request_id, "select-1") == 0);
    CHECK(select_capture.status == CMD_STATUS_OK);
    CHECK(select_capture.ok == 1);
    CHECK(select_capture.body_format == CMD_BODY_TEXT);
    CHECK(strcmp(select_capture.body, "SELECT matched_rows=1") == 0);
    CHECK(select_capture.row_count == 1);

    cmd_processor_shutdown(processor);
    cleanup_test_files();
    return 0;
}

static int test_parse_error(void) {
    CmdProcessor *processor = NULL;
    CallbackCapture capture;

    init_capture(&capture, 1);
    CHECK(repl_cmd_processor_create(NULL, &processor) == 0);
    CHECK(submit_sql(processor, "bad-sql", "BOGUS SQL", &capture) == 0);
    CHECK(capture.called == 1);
    CHECK(strcmp(capture.request_id, "bad-sql") == 0);
    CHECK(capture.status == CMD_STATUS_PARSE_ERROR);
    CHECK(capture.ok == 0);
    CHECK(capture.error_message[0] != '\0');
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_sql_length_limit(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;

    memset(&context, 0, sizeof(context));
    context.max_sql_len = 4;
    context.request_buffer_count = 1;
    context.response_body_capacity = 64;

    CHECK(repl_cmd_processor_create(&context, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "ok", "1234") == CMD_STATUS_OK);
    CHECK(cmd_processor_set_sql_request(processor, request, "too-long", "12345") == CMD_STATUS_SQL_TOO_LONG);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_response_capacity_exceeded(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CallbackCapture capture;

    CHECK(write_test_table() == 0);
    memset(&context, 0, sizeof(context));
    context.request_buffer_count = 1;
    context.response_body_capacity = 8;

    init_capture(&capture, 1);
    CHECK(repl_cmd_processor_create(&context, &processor) == 0);
    CHECK(submit_sql(processor,
                     "small-body",
                     "INSERT INTO " TEST_TABLE " VALUES (1,'lee')",
                     &capture) == 0);
    CHECK(capture.called == 1);
    CHECK(capture.status == CMD_STATUS_PROCESSING_ERROR);
    CHECK(capture.ok == 0);
    CHECK(strcmp(capture.error_message, "response body capacity exceeded") == 0);

    cmd_processor_shutdown(processor);
    cleanup_test_files();
    return 0;
}

static int test_request_pool_reuse(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CmdRequest *first = NULL;
    CmdRequest *second = NULL;

    memset(&context, 0, sizeof(context));
    context.request_buffer_count = 1;
    context.response_body_capacity = 64;

    CHECK(repl_cmd_processor_create(&context, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &first) == 0);
    CHECK(cmd_processor_acquire_request(processor, &second) == -1);
    CHECK(second == NULL);
    cmd_processor_release_request(processor, first);
    CHECK(cmd_processor_acquire_request(processor, &second) == 0);
    CHECK(second == first);
    cmd_processor_release_request(processor, second);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_rejects_foreign_request(void) {
    CmdProcessor *processor = NULL;
    CmdRequest request;
    CallbackCapture capture;

    init_capture(&capture, 0);
    memset(&request, 0, sizeof(request));
    request.type = CMD_REQUEST_SQL;
    request.sql = "SELECT 1";

    CHECK(repl_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_submit(processor, &request, capture_callback, &capture) == -1);
    CHECK(capture.called == 0);
    cmd_processor_shutdown(processor);
    return 0;
}

static db_thread_return_t DB_THREAD_CALL ping_worker(void *arg_ptr) {
    WorkerArg *arg = (WorkerArg *)arg_ptr;
    int i;

    for (i = 0; i < 100; i++) {
        CallbackCapture capture;
        char request_id[64];

        init_capture(&capture, 1);
        snprintf(request_id, sizeof(request_id), "t%d-%d", arg->thread_index, i);
        if (submit_ping(arg->processor, request_id, &capture) != 0 ||
            capture.called != 1 ||
            strcmp(capture.request_id, request_id) != 0 ||
            capture.status != CMD_STATUS_OK ||
            strcmp(capture.body, "pong") != 0) {
            arg->failed = 1;
            break;
        }
    }
#if defined(_WIN32)
    return 0;
#else
    return NULL;
#endif
}

static int test_concurrent_ping_submit(void) {
    enum { THREAD_COUNT = 4 };
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    db_thread_t threads[THREAD_COUNT];
    WorkerArg args[THREAD_COUNT];
    int i;

    memset(&context, 0, sizeof(context));
    context.request_buffer_count = THREAD_COUNT;
    context.response_body_capacity = 64;

    CHECK(repl_cmd_processor_create(&context, &processor) == 0);
    for (i = 0; i < THREAD_COUNT; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].processor = processor;
        args[i].thread_index = i;
        CHECK(db_thread_create(&threads[i], ping_worker, &args[i]) == 1);
    }
    for (i = 0; i < THREAD_COUNT; i++) {
        db_thread_join(threads[i]);
        CHECK(args[i].failed == 0);
    }

    cmd_processor_shutdown(processor);
    return 0;
}

int main(void) {
    set_executor_quiet(1);
    cleanup_test_files();

    CHECK(test_default_context() == 0);
    CHECK(test_context_override() == 0);
    CHECK(test_ping() == 0);
    CHECK(test_sql_insert_and_select() == 0);
    CHECK(test_parse_error() == 0);
    CHECK(test_sql_length_limit() == 0);
    CHECK(test_response_capacity_exceeded() == 0);
    CHECK(test_request_pool_reuse() == 0);
    CHECK(test_rejects_foreign_request() == 0);
    CHECK(test_concurrent_ping_submit() == 0);

    cleanup_test_files();
    set_executor_quiet(0);
    printf("[ok] repl_cmd_processor checks passed\n");
    return 0;
}
