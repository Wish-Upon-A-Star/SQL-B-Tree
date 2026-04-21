#include "cmd_processor.h"
#include "mock_cmd_processor.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[fail] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

typedef struct {
    CmdProcessor *processor;
    int thread_index;
    int failed;
} WorkerArg;

static int test_status_strings(void) {
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_OK), "OK") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_BAD_REQUEST), "BAD_REQUEST") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_SQL_TOO_LONG), "SQL_TOO_LONG") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_PARSE_ERROR), "PARSE_ERROR") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_PROCESSING_ERROR), "PROCESSING_ERROR") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_BUSY), "BUSY") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_TIMEOUT), "TIMEOUT") == 0);
    CHECK(strcmp(cmd_status_to_string(CMD_STATUS_INTERNAL_ERROR), "INTERNAL_ERROR") == 0);
    CHECK(strcmp(cmd_status_to_string((CmdStatusCode)999), "UNKNOWN") == 0);
    return 0;
}

static int test_default_context(void) {
    CmdProcessor *processor = NULL;

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(processor != NULL);
    CHECK(processor->context != NULL);
    CHECK(strcmp(processor->context->name, "mock_cmd_processor") == 0);
    CHECK(processor->context->max_sql_len == 4096);
    CHECK(processor->context->request_buffer_count == 16);
    CHECK(processor->context->response_body_capacity == 4096);
    CHECK(processor->context->shared_state != NULL);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_ping(void) {
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_ping_request(processor, request, "ping-1") == CMD_STATUS_OK);
    CHECK(cmd_processor_process(processor, request, &response) == 0);
    CHECK(response != NULL);
    CHECK(strcmp(response->request_id, "ping-1") == 0);
    CHECK(response->status == CMD_STATUS_OK);
    CHECK(response->ok == 1);
    CHECK(response->body_format == CMD_BODY_TEXT);
    CHECK(response->body != NULL);
    CHECK(strcmp(response->body, "pong") == 0);
    CHECK(response->body_len == 4);
    CHECK(response->error_message == NULL);
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_sql_json_echo_and_copy(void) {
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;
    char sql[] = "SELECT * FROM users;";
    const char *expected = "{\"mock\":true,\"sql\":\"SELECT * FROM users;\"}";

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "sql-1", sql) == CMD_STATUS_OK);
    strcpy(sql, "CHANGED");
    CHECK(strcmp(request->sql, "SELECT * FROM users;") == 0);
    CHECK(cmd_processor_process(processor, request, &response) == 0);
    CHECK(response != NULL);
    CHECK(strcmp(response->request_id, "sql-1") == 0);
    CHECK(response->status == CMD_STATUS_OK);
    CHECK(response->ok == 1);
    CHECK(response->body_format == CMD_BODY_JSON);
    CHECK(response->body != NULL);
    CHECK(strcmp(response->body, expected) == 0);
    CHECK(response->body_len == strlen(expected));
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_json_escape(void) {
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;
    const char *expected = "{\"mock\":true,\"sql\":\"SELECT \\\"x\\\" \\\\ \\n\"}";

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "escape", "SELECT \"x\" \\ \n") == CMD_STATUS_OK);
    CHECK(cmd_processor_process(processor, request, &response) == 0);
    CHECK(response != NULL);
    CHECK(strcmp(response->body, expected) == 0);
    CHECK(response->body_len == strlen(expected));
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_sql_length_limit(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;

    memset(&context, 0, sizeof(context));
    context.max_sql_len = 4;
    context.request_buffer_count = 2;
    context.response_body_capacity = 64;

    CHECK(mock_cmd_processor_create(&context, &processor) == 0);
    CHECK(processor->context->max_sql_len == 4);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "ok", "1234") == CMD_STATUS_OK);
    CHECK(strcmp(request->sql, "1234") == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "too-long", "12345") == CMD_STATUS_SQL_TOO_LONG);
    CHECK(strcmp(request->sql, "1234") == 0);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_error_response_helper(void) {
    CmdProcessor *processor = NULL;
    CmdResponse *response = NULL;

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_make_error_response(processor,
                                            "bad-1",
                                            CMD_STATUS_BAD_REQUEST,
                                            "missing sql",
                                            &response) == 0);
    CHECK(response != NULL);
    CHECK(strcmp(response->request_id, "bad-1") == 0);
    CHECK(response->status == CMD_STATUS_BAD_REQUEST);
    CHECK(response->ok == 0);
    CHECK(response->body_format == CMD_BODY_NONE);
    CHECK(response->body == NULL);
    CHECK(response->body_len == 0);
    CHECK(response->error_message != NULL);
    CHECK(strcmp(response->error_message, "missing sql") == 0);
    cmd_processor_release_response(processor, response);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_rejects_foreign_request(void) {
    CmdProcessor *processor = NULL;
    CmdRequest request;
    CmdResponse *response = NULL;

    memset(&request, 0, sizeof(request));
    request.sql = "SELECT 1;";

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_process(processor, &request, &response) == -1);
    CHECK(response == NULL);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_request_pool_reuse(void) {
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    CmdRequest *first = NULL;
    CmdRequest *second = NULL;
    CmdRequest *third = NULL;

    memset(&context, 0, sizeof(context));
    context.max_sql_len = 32;
    context.request_buffer_count = 2;
    context.response_body_capacity = 64;

    CHECK(mock_cmd_processor_create(&context, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &first) == 0);
    CHECK(cmd_processor_acquire_request(processor, &second) == 0);
    CHECK(cmd_processor_acquire_request(processor, &third) == -1);
    CHECK(third == NULL);
    cmd_processor_release_request(processor, first);
    CHECK(cmd_processor_acquire_request(processor, &third) == 0);
    CHECK(third == first);
    cmd_processor_release_request(processor, second);
    cmd_processor_release_request(processor, third);
    cmd_processor_shutdown(processor);
    return 0;
}

static int expect_mock_status(const char *sql, CmdStatusCode status) {
    CmdProcessor *processor = NULL;
    CmdRequest *request = NULL;
    CmdResponse *response = NULL;

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    CHECK(cmd_processor_acquire_request(processor, &request) == 0);
    CHECK(cmd_processor_set_sql_request(processor, request, "status", sql) == CMD_STATUS_OK);
    CHECK(cmd_processor_process(processor, request, &response) == 0);
    CHECK(response != NULL);
    CHECK(response->status == status);
    CHECK(response->ok == 0);
    CHECK(response->body_format == CMD_BODY_NONE);
    CHECK(response->error_message != NULL);
    cmd_processor_release_response(processor, response);
    cmd_processor_release_request(processor, request);
    cmd_processor_shutdown(processor);
    return 0;
}

static void *worker_main(void *arg_ptr) {
    WorkerArg *arg = (WorkerArg *)arg_ptr;
    int i;

    for (i = 0; i < 200; i++) {
        CmdRequest *request = NULL;
        CmdResponse *response = NULL;
        char request_id[64];
        char sql[128];

        snprintf(request_id, sizeof(request_id), "t%d-%d", arg->thread_index, i);
        snprintf(sql, sizeof(sql), "SELECT %d FROM worker_%d;", i, arg->thread_index);

        if (cmd_processor_acquire_request(arg->processor, &request) != 0) {
            arg->failed = 1;
            return NULL;
        }
        if (cmd_processor_set_sql_request(arg->processor, request, request_id, sql) != CMD_STATUS_OK) {
            arg->failed = 1;
            cmd_processor_release_request(arg->processor, request);
            return NULL;
        }
        if (cmd_processor_process(arg->processor, request, &response) != 0 || !response) {
            arg->failed = 1;
            cmd_processor_release_request(arg->processor, request);
            return NULL;
        }
        if (strcmp(response->request_id, request_id) != 0 ||
            response->status != CMD_STATUS_OK ||
            response->ok != 1 ||
            response->body_format != CMD_BODY_JSON ||
            !response->body ||
            strstr(response->body, sql) == NULL ||
            response->body_len != strlen(response->body)) {
            arg->failed = 1;
            cmd_processor_release_response(arg->processor, response);
            cmd_processor_release_request(arg->processor, request);
            return NULL;
        }

        cmd_processor_release_response(arg->processor, response);
        cmd_processor_release_request(arg->processor, request);
    }

    return NULL;
}

static int test_concurrent_process_calls(void) {
    enum { THREAD_COUNT = 4 };
    CmdProcessorContext context;
    CmdProcessor *processor = NULL;
    pthread_t threads[THREAD_COUNT];
    WorkerArg args[THREAD_COUNT];
    int i;

    memset(&context, 0, sizeof(context));
    context.max_sql_len = 256;
    context.request_buffer_count = THREAD_COUNT;
    context.response_body_capacity = 512;

    CHECK(mock_cmd_processor_create(&context, &processor) == 0);
    for (i = 0; i < THREAD_COUNT; i++) {
        args[i].processor = processor;
        args[i].thread_index = i;
        args[i].failed = 0;
        CHECK(pthread_create(&threads[i], NULL, worker_main, &args[i]) == 0);
    }
    for (i = 0; i < THREAD_COUNT; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0);
        CHECK(args[i].failed == 0);
    }

    cmd_processor_shutdown(processor);
    return 0;
}

int main(void) {
    CHECK(test_status_strings() == 0);
    CHECK(test_default_context() == 0);
    CHECK(test_ping() == 0);
    CHECK(test_sql_json_echo_and_copy() == 0);
    CHECK(test_json_escape() == 0);
    CHECK(test_sql_length_limit() == 0);
    CHECK(test_error_response_helper() == 0);
    CHECK(test_rejects_foreign_request() == 0);
    CHECK(test_request_pool_reuse() == 0);
    CHECK(expect_mock_status("MOCK_BUSY", CMD_STATUS_BUSY) == 0);
    CHECK(expect_mock_status("MOCK_TIMEOUT", CMD_STATUS_TIMEOUT) == 0);
    CHECK(expect_mock_status("MOCK_PROCESSING_ERROR", CMD_STATUS_PROCESSING_ERROR) == 0);
    CHECK(test_concurrent_process_calls() == 0);

    printf("[ok] cmd_processor checks passed\n");
    return 0;
}
