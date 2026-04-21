#include "tcp_cmd_processor.h"

#include "../cmd_processor/mock_cmd_processor.h"
#include "../thirdparty/cjson/cJSON.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "[fail] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

typedef struct {
    CmdProcessor processor;
    CmdProcessorContext context;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int release_all;
    size_t pending_count;
    size_t active_jobs;
    size_t max_sql_len;
} DelayedProcessorState;

typedef struct {
    DelayedProcessorState *state;
    CmdProcessor *processor;
    CmdRequest *request;
    CmdProcessorResponseCallback callback;
    void *user_data;
} DelayedJob;

static void copy_fixed(char *dst, size_t dst_size, const char *src) {
    size_t len = 0;

    if (!dst || dst_size == 0) return;
    if (src) len = strlen(src);
    if (len >= dst_size) len = dst_size - 1;
    if (len > 0) memcpy(dst, src, len);
    dst[len] = '\0';
}

static DelayedProcessorState *delayed_state_from_context(CmdProcessorContext *context) {
    if (!context) return NULL;
    return (DelayedProcessorState *)context->shared_state;
}

static int delayed_acquire_request(CmdProcessorContext *context,
                                   CmdRequest **out_request) {
    DelayedProcessorState *state;
    CmdRequest *request;

    if (out_request) *out_request = NULL;
    state = delayed_state_from_context(context);
    if (!state || !out_request) return -1;

    request = (CmdRequest *)calloc(1, sizeof(*request));
    if (!request) return -1;
    request->sql = (char *)calloc(state->max_sql_len + 1, 1);
    if (!request->sql) {
        free(request);
        return -1;
    }
    request->type = (CmdRequestType)-1;
    *out_request = request;
    return 0;
}

static CmdResponse *delayed_make_response(CmdRequest *request) {
    CmdResponse *response;
    cJSON *body;
    char *body_json;

    response = (CmdResponse *)calloc(1, sizeof(*response));
    if (!response) return NULL;

    copy_fixed(response->request_id, sizeof(response->request_id), request->request_id);
    response->status = CMD_STATUS_OK;
    response->ok = 1;
    response->body_format = CMD_BODY_NONE;

    if (request->type == CMD_REQUEST_PING) {
        response->body = (char *)calloc(5, 1);
        if (!response->body) {
            free(response);
            return NULL;
        }
        memcpy(response->body, "pong", 5);
        response->body_len = 4;
        response->body_format = CMD_BODY_TEXT;
        return response;
    }

    body = cJSON_CreateObject();
    if (!body) {
        free(response);
        return NULL;
    }
    cJSON_AddBoolToObject(body, "delayed", 1);
    cJSON_AddStringToObject(body, "sql", request->sql ? request->sql : "");
    body_json = cJSON_PrintUnformatted(body);
    cJSON_Delete(body);
    if (!body_json) {
        free(response);
        return NULL;
    }

    response->body_len = strlen(body_json);
    response->body = (char *)calloc(response->body_len + 1, 1);
    if (!response->body) {
        cJSON_free(body_json);
        free(response);
        return NULL;
    }
    memcpy(response->body, body_json, response->body_len + 1);
    response->body_format = CMD_BODY_JSON;
    cJSON_free(body_json);
    return response;
}

static void *delayed_worker_main(void *arg) {
    DelayedJob *job = (DelayedJob *)arg;
    CmdResponse *response;
    int should_delay;

    pthread_mutex_lock(&job->state->mutex);
    while (!job->state->release_all) {
        pthread_cond_wait(&job->state->cond, &job->state->mutex);
    }
    pthread_mutex_unlock(&job->state->mutex);

    should_delay = job->request &&
                   job->request->sql &&
                   strcmp(job->request->sql, "delay-first") == 0;
    if (should_delay) usleep(200000);

    response = delayed_make_response(job->request);
    job->callback(job->processor, job->request, response, job->user_data);

    pthread_mutex_lock(&job->state->mutex);
    if (job->state->pending_count > 0) job->state->pending_count--;
    if (job->state->active_jobs > 0) job->state->active_jobs--;
    pthread_cond_broadcast(&job->state->cond);
    pthread_mutex_unlock(&job->state->mutex);

    free(job);
    return NULL;
}

static int delayed_submit(CmdProcessor *processor,
                          CmdProcessorContext *context,
                          CmdRequest *request,
                          CmdProcessorResponseCallback callback,
                          void *user_data) {
    DelayedProcessorState *state;
    DelayedJob *job;
    pthread_t thread;

    state = delayed_state_from_context(context);
    if (!processor || !state || !request || !callback) return -1;

    job = (DelayedJob *)calloc(1, sizeof(*job));
    if (!job) return -1;
    job->state = state;
    job->processor = processor;
    job->request = request;
    job->callback = callback;
    job->user_data = user_data;

    pthread_mutex_lock(&state->mutex);
    state->pending_count++;
    state->active_jobs++;
    pthread_mutex_unlock(&state->mutex);

    if (pthread_create(&thread, NULL, delayed_worker_main, job) != 0) {
        pthread_mutex_lock(&state->mutex);
        state->pending_count--;
        state->active_jobs--;
        pthread_mutex_unlock(&state->mutex);
        free(job);
        return -1;
    }
    pthread_detach(thread);
    return 0;
}

static int delayed_make_error_response(CmdProcessorContext *context,
                                       const char *request_id,
                                       CmdStatusCode status,
                                       const char *error_message,
                                       CmdResponse **out_response) {
    CmdResponse *response;

    (void)context;
    if (out_response) *out_response = NULL;
    if (!out_response) return -1;

    response = (CmdResponse *)calloc(1, sizeof(*response));
    if (!response) return -1;
    copy_fixed(response->request_id, sizeof(response->request_id), request_id);
    response->status = status;
    response->ok = 0;
    response->body_format = CMD_BODY_NONE;
    if (error_message) {
        response->error_message = (char *)calloc(strlen(error_message) + 1, 1);
        if (!response->error_message) {
            free(response);
            return -1;
        }
        strcpy(response->error_message, error_message);
    }
    *out_response = response;
    return 0;
}

static void delayed_release_request(CmdProcessorContext *context,
                                    CmdRequest *request) {
    (void)context;
    if (!request) return;
    free(request->sql);
    free(request);
}

static void delayed_release_response(CmdProcessorContext *context,
                                     CmdResponse *response) {
    (void)context;
    if (!response) return;
    free(response->body);
    free(response->error_message);
    free(response);
}

static void delayed_shutdown(CmdProcessorContext *context) {
    DelayedProcessorState *state;

    state = delayed_state_from_context(context);
    if (!state) return;

    pthread_mutex_lock(&state->mutex);
    state->release_all = 1;
    pthread_cond_broadcast(&state->cond);
    while (state->active_jobs > 0) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);

    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
    free(state);
}

static int delayed_processor_create(CmdProcessor **out_processor,
                                    DelayedProcessorState **out_state) {
    DelayedProcessorState *state;

    if (out_processor) *out_processor = NULL;
    if (out_state) *out_state = NULL;
    if (!out_processor || !out_state) return -1;

    state = (DelayedProcessorState *)calloc(1, sizeof(*state));
    if (!state) return -1;
    if (pthread_mutex_init(&state->mutex, NULL) != 0) {
        free(state);
        return -1;
    }
    if (pthread_cond_init(&state->cond, NULL) != 0) {
        pthread_mutex_destroy(&state->mutex);
        free(state);
        return -1;
    }

    state->max_sql_len = 4096;
    state->context.name = "delayed_cmd_processor";
    state->context.max_sql_len = state->max_sql_len;
    state->context.request_buffer_count = 128;
    state->context.response_body_capacity = 4096;
    state->context.shared_state = state;
    state->processor.context = &state->context;
    state->processor.acquire_request = delayed_acquire_request;
    state->processor.submit = delayed_submit;
    state->processor.make_error_response = delayed_make_error_response;
    state->processor.release_request = delayed_release_request;
    state->processor.release_response = delayed_release_response;
    state->processor.shutdown = delayed_shutdown;

    *out_processor = &state->processor;
    *out_state = state;
    return 0;
}

static void delayed_release_all(DelayedProcessorState *state) {
    pthread_mutex_lock(&state->mutex);
    state->release_all = 1;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static int delayed_wait_pending(DelayedProcessorState *state, size_t expected) {
    int i;

    for (i = 0; i < 200; i++) {
        int ok;
        pthread_mutex_lock(&state->mutex);
        ok = state->pending_count >= expected;
        pthread_mutex_unlock(&state->mutex);
        if (ok) return 1;
        usleep(10000);
    }
    return 0;
}

static void set_client_timeout(int fd) {
    struct timeval timeout;
#ifdef SO_NOSIGPIPE
    int no_sigpipe = 1;
#endif

    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
    (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#endif
}

static int connect_client(int port) {
    int fd;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_client_timeout(fd);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_all_client(int fd, const char *data, size_t len) {
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int send_line(int fd, const char *line) {
    if (send_all_client(fd, line, strlen(line)) != 0) return -1;
    return send_all_client(fd, "\n", 1);
}

static int read_line(int fd, char *buffer, size_t buffer_size) {
    size_t len = 0;

    if (!buffer || buffer_size == 0) return -1;
    while (len + 1 < buffer_size) {
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n == 0) return -1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (ch == '\n') {
            if (len > 0 && buffer[len - 1] == '\r') len--;
            buffer[len] = '\0';
            return (int)len;
        }
        buffer[len++] = ch;
    }
    return -1;
}

static cJSON *read_json_response(int fd) {
    char line[8192];

    if (read_line(fd, line, sizeof(line)) < 0) return NULL;
    return cJSON_Parse(line);
}

static int json_string_equals(cJSON *root, const char *name, const char *expected) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    return cJSON_IsString(item) && strcmp(item->valuestring, expected) == 0;
}

static int json_bool_equals(cJSON *root, const char *name, int expected) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, name);
    if (expected) return cJSON_IsTrue(item);
    return cJSON_IsFalse(item);
}

static int expect_response_status(int fd,
                                  const char *id,
                                  const char *status,
                                  int ok) {
    cJSON *root;

    root = read_json_response(fd);
    CHECK(root != NULL);
    CHECK(json_string_equals(root, "id", id));
    CHECK(json_string_equals(root, "status", status));
    CHECK(json_bool_equals(root, "ok", ok));
    cJSON_Delete(root);
    return 0;
}

static int expect_two_ok_ids(int fd, const char *first_id, const char *second_id) {
    cJSON *first;
    cJSON *second;
    int saw_first;
    int saw_second;

    first = read_json_response(fd);
    second = read_json_response(fd);
    CHECK(first != NULL);
    CHECK(second != NULL);
    CHECK(json_string_equals(first, "status", "OK"));
    CHECK(json_string_equals(second, "status", "OK"));
    CHECK(json_bool_equals(first, "ok", 1));
    CHECK(json_bool_equals(second, "ok", 1));

    saw_first = json_string_equals(first, "id", first_id) ||
                json_string_equals(second, "id", first_id);
    saw_second = json_string_equals(first, "id", second_id) ||
                 json_string_equals(second, "id", second_id);
    CHECK(saw_first);
    CHECK(saw_second);

    cJSON_Delete(first);
    cJSON_Delete(second);
    return 0;
}

static int start_mock_server(CmdProcessor **out_processor,
                             TCPCmdProcessor **out_server,
                             int *out_port) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    TCPCmdProcessorConfig config;
    int port;

    CHECK(mock_cmd_processor_create(NULL, &processor) == 0);
    tcp_cmd_processor_config_init(&config, processor);
    CHECK(tcp_cmd_processor_start(&config, &server) == 0);
    port = tcp_cmd_processor_get_port(server);
    CHECK(port > 0);

    *out_processor = processor;
    *out_server = server;
    *out_port = port;
    return 0;
}

static int test_start_port_and_ping(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int fd;
    cJSON *root;
    cJSON *body;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);
    CHECK(send_line(fd, "{\"id\":\"p1\",\"op\":\"ping\"}") == 0);
    root = read_json_response(fd);
    CHECK(root != NULL);
    CHECK(json_string_equals(root, "id", "p1"));
    CHECK(json_string_equals(root, "status", "OK"));
    CHECK(json_bool_equals(root, "ok", 1));
    body = cJSON_GetObjectItemCaseSensitive(root, "body");
    CHECK(cJSON_IsString(body));
    CHECK(strcmp(body->valuestring, "pong") == 0);
    cJSON_Delete(root);
    close(fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_sql_and_multiple_requests(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int fd;
    cJSON *first;
    cJSON *second;
    cJSON *body;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);
    CHECK(send_line(fd, "{\"id\":\"s1\",\"op\":\"sql\",\"sql\":\"SELECT 1;\"}") == 0);
    CHECK(send_line(fd, "{\"id\":\"p2\",\"op\":\"ping\"}") == 0);

    first = read_json_response(fd);
    second = read_json_response(fd);
    CHECK(first != NULL);
    CHECK(second != NULL);
    CHECK(json_string_equals(first, "id", "s1"));
    CHECK(json_string_equals(first, "status", "OK"));
    body = cJSON_GetObjectItemCaseSensitive(first, "body");
    CHECK(cJSON_IsObject(body));
    CHECK(json_string_equals(body, "sql", "SELECT 1;"));
    CHECK(json_string_equals(second, "id", "p2"));
    CHECK(json_string_equals(second, "status", "OK"));

    cJSON_Delete(first);
    cJSON_Delete(second);
    close(fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_invalid_requests(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int fd;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);

    CHECK(send_line(fd, "{bad json") == 0);
    CHECK(expect_response_status(fd, "unknown", "BAD_REQUEST", 0) == 0);
    CHECK(send_line(fd, "{\"op\":\"ping\"}") == 0);
    CHECK(expect_response_status(fd, "unknown", "BAD_REQUEST", 0) == 0);
    CHECK(send_line(fd, "{\"id\":\"m1\"}") == 0);
    CHECK(expect_response_status(fd, "m1", "BAD_REQUEST", 0) == 0);
    CHECK(send_line(fd, "{\"id\":\"u1\",\"op\":\"unknown\"}") == 0);
    CHECK(expect_response_status(fd, "u1", "BAD_REQUEST", 0) == 0);
    CHECK(send_line(fd, "{\"id\":\"s1\",\"op\":\"sql\"}") == 0);
    CHECK(expect_response_status(fd, "s1", "BAD_REQUEST", 0) == 0);

    close(fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_close_only_current_connection(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int first_fd;
    int second_fd;
    char line[256];

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    first_fd = connect_client(port);
    second_fd = connect_client(port);
    CHECK(first_fd >= 0);
    CHECK(second_fd >= 0);

    CHECK(send_line(first_fd, "{\"id\":\"c1\",\"op\":\"close\"}") == 0);
    CHECK(expect_response_status(first_fd, "c1", "OK", 1) == 0);
    CHECK(read_line(first_fd, line, sizeof(line)) < 0);

    CHECK(send_line(second_fd, "{\"id\":\"p2\",\"op\":\"ping\"}") == 0);
    CHECK(expect_response_status(second_fd, "p2", "OK", 1) == 0);

    close(first_fd);
    close(second_fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_connection_limit_per_client(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int first_fd;
    int second_fd;
    int third_fd;
    char line[256];

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    first_fd = connect_client(port);
    second_fd = connect_client(port);
    CHECK(first_fd >= 0);
    CHECK(second_fd >= 0);
    CHECK(send_line(first_fd, "{\"id\":\"p1\",\"op\":\"ping\"}") == 0);
    CHECK(send_line(second_fd, "{\"id\":\"p2\",\"op\":\"ping\"}") == 0);
    CHECK(expect_response_status(first_fd, "p1", "OK", 1) == 0);
    CHECK(expect_response_status(second_fd, "p2", "OK", 1) == 0);

    third_fd = connect_client(port);
    CHECK(third_fd >= 0);
    (void)send_line(third_fd, "{\"id\":\"p3\",\"op\":\"ping\"}");
    CHECK(read_line(third_fd, line, sizeof(line)) < 0);

    close(first_fd);
    close(second_fd);
    close(third_fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_inflight_per_connection_limit_and_out_of_order(void) {
    CmdProcessor *processor = NULL;
    DelayedProcessorState *state = NULL;
    TCPCmdProcessor *server = NULL;
    TCPCmdProcessorConfig config;
    int port;
    int fd;
    cJSON *first;
    cJSON *second;

    CHECK(delayed_processor_create(&processor, &state) == 0);
    tcp_cmd_processor_config_init(&config, processor);
    CHECK(tcp_cmd_processor_start(&config, &server) == 0);
    port = tcp_cmd_processor_get_port(server);
    CHECK(port > 0);
    fd = connect_client(port);
    CHECK(fd >= 0);

    CHECK(send_line(fd, "{\"id\":\"r1\",\"op\":\"sql\",\"sql\":\"delay-first\"}") == 0);
    CHECK(send_line(fd, "{\"id\":\"r2\",\"op\":\"sql\",\"sql\":\"fast-second\"}") == 0);
    CHECK(delayed_wait_pending(state, 2));
    CHECK(send_line(fd, "{\"id\":\"r3\",\"op\":\"sql\",\"sql\":\"third\"}") == 0);
    CHECK(expect_response_status(fd, "r3", "BUSY", 0) == 0);

    delayed_release_all(state);
    first = read_json_response(fd);
    second = read_json_response(fd);
    CHECK(first != NULL);
    CHECK(second != NULL);
    CHECK(json_string_equals(first, "id", "r2"));
    CHECK(json_string_equals(second, "id", "r1"));
    CHECK(json_string_equals(first, "status", "OK"));
    CHECK(json_string_equals(second, "status", "OK"));

    cJSON_Delete(first);
    cJSON_Delete(second);
    close(fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_inflight_per_client_limit(void) {
    CmdProcessor *processor = NULL;
    DelayedProcessorState *state = NULL;
    TCPCmdProcessor *server = NULL;
    TCPCmdProcessorConfig config;
    int port;
    int first_fd;
    int second_fd;

    CHECK(delayed_processor_create(&processor, &state) == 0);
    tcp_cmd_processor_config_init(&config, processor);
    CHECK(tcp_cmd_processor_start(&config, &server) == 0);
    port = tcp_cmd_processor_get_port(server);
    CHECK(port > 0);
    first_fd = connect_client(port);
    second_fd = connect_client(port);
    CHECK(first_fd >= 0);
    CHECK(second_fd >= 0);

    CHECK(send_line(first_fd, "{\"id\":\"a1\",\"op\":\"sql\",\"sql\":\"one\"}") == 0);
    CHECK(send_line(first_fd, "{\"id\":\"a2\",\"op\":\"sql\",\"sql\":\"two\"}") == 0);
    CHECK(delayed_wait_pending(state, 2));
    CHECK(send_line(second_fd, "{\"id\":\"b1\",\"op\":\"sql\",\"sql\":\"three\"}") == 0);
    CHECK(delayed_wait_pending(state, 3));
    CHECK(send_line(second_fd, "{\"id\":\"b2\",\"op\":\"sql\",\"sql\":\"four\"}") == 0);
    CHECK(expect_response_status(second_fd, "b2", "BUSY", 0) == 0);

    delayed_release_all(state);
    CHECK(expect_two_ok_ids(first_fd, "a1", "a2") == 0);
    CHECK(expect_response_status(second_fd, "b1", "OK", 1) == 0);

    close(first_fd);
    close(second_fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

int main(void) {
    CHECK(test_start_port_and_ping() == 0);
    CHECK(test_sql_and_multiple_requests() == 0);
    CHECK(test_invalid_requests() == 0);
    CHECK(test_close_only_current_connection() == 0);
    CHECK(test_connection_limit_per_client() == 0);
    CHECK(test_inflight_per_connection_limit_and_out_of_order() == 0);
    CHECK(test_inflight_per_client_limit() == 0);

    printf("[ok] tcp_cmd_processor checks passed\n");
    return 0;
}
