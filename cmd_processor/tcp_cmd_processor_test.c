#include "tcp_cmd_processor.h"
#include "tcp_protocol_binary.h"

#include "mock_cmd_processor.h"
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

static int recv_all_client(int fd, unsigned char *data, size_t len) {
    size_t received = 0;

    while (received < len) {
        ssize_t n = recv(fd, data + received, len - received, 0);
        if (n > 0) {
            received += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int send_request_frame(int fd,
                              TCPBinaryOp op,
                              const char *request_id,
                              const char *payload,
                              size_t payload_len) {
    TCPBinaryRequestHeader header;
    size_t header_size;
    size_t request_id_len;
    size_t frame_len;
    unsigned char *frame;
    unsigned char *cursor;
    int rc;

    request_id_len = request_id ? strlen(request_id) : 0;
    tcp_binary_init_request_header(&header, op, request_id_len, payload_len);
    frame_len = tcp_binary_request_frame_size(&header);
    header_size = tcp_binary_request_header_size();
    frame = (unsigned char *)calloc(frame_len, 1);
    if (!frame) return -1;

    tcp_binary_encode_request_header(frame, &header);
    cursor = frame + header_size;
    if (request_id_len > 0) {
        memcpy(cursor, request_id, request_id_len);
        cursor += request_id_len;
    }
    if (payload_len > 0) memcpy(cursor, payload, payload_len);
    rc = send_all_client(fd, (const char *)frame, frame_len);
    free(frame);
    return rc;
}

static int send_ping_request(int fd, const char *request_id) {
    return send_request_frame(fd, TCP_BINARY_OP_PING, request_id, NULL, 0);
}

static int send_close_request(int fd, const char *request_id) {
    return send_request_frame(fd, TCP_BINARY_OP_CLOSE, request_id, NULL, 0);
}

static int send_sql_request(int fd, const char *request_id, const char *sql) {
    return send_request_frame(fd,
                              TCP_BINARY_OP_SQL,
                              request_id,
                              sql,
                              sql ? strlen(sql) : 0);
}

static int test_binary_request_header_roundtrip(void) {
    TCPBinaryRequestHeader header;
    TCPBinaryRequestHeader decoded;
    unsigned char bytes[32];

    tcp_binary_init_request_header(&header, TCP_BINARY_OP_SQL, 7, 128);
    memset(bytes, 0, sizeof(bytes));
    tcp_binary_encode_request_header(bytes, &header);
    CHECK(tcp_binary_decode_request_header(bytes, tcp_binary_request_header_size(), &decoded) == 1);
    CHECK(decoded.magic == TCP_PROTOCOL_REQUEST_MAGIC);
    CHECK(decoded.version == TCP_PROTOCOL_VERSION);
    CHECK(decoded.op == TCP_BINARY_OP_SQL);
    CHECK(decoded.request_id_len == 7);
    CHECK(decoded.payload_len == 128);
    CHECK(tcp_binary_validate_request_header(&decoded) == 1);
    return 0;
}

static int test_binary_response_header_roundtrip(void) {
    TCPBinaryResponseHeader header;
    TCPBinaryResponseHeader decoded;
    unsigned char bytes[64];

    tcp_binary_init_response_header(&header,
                                    CMD_STATUS_OK,
                                    1,
                                    CMD_BODY_BINARY,
                                    6,
                                    48,
                                    0,
                                    3,
                                    2);
    memset(bytes, 0, sizeof(bytes));
    tcp_binary_encode_response_header(bytes, &header);
    CHECK(tcp_binary_decode_response_header(bytes, tcp_binary_response_header_size(), &decoded) == 1);
    CHECK(decoded.magic == TCP_PROTOCOL_RESPONSE_MAGIC);
    CHECK(decoded.version == TCP_PROTOCOL_VERSION);
    CHECK(decoded.status == CMD_STATUS_OK);
    CHECK(decoded.flags == 1u);
    CHECK(decoded.request_id_len == 6);
    CHECK(decoded.body_format == CMD_BODY_BINARY);
    CHECK(decoded.body_len == 48);
    CHECK(decoded.error_len == 0);
    CHECK(decoded.row_count == 3);
    CHECK(decoded.affected_count == 2);
    CHECK(tcp_binary_validate_response_header(&decoded) == 1);
    return 0;
}

static int test_binary_request_frame_decode(void) {
    TCPBinaryRequestHeader header;
    TCPBinaryDecodedRequest decoded;
    unsigned char frame[128];
    const char *request_id = "req-42";
    const char *payload = "SELECT 1";
    size_t header_size = tcp_binary_request_header_size();
    size_t request_id_len = strlen(request_id);
    size_t payload_len = strlen(payload);

    tcp_binary_init_request_header(&header, TCP_BINARY_OP_SQL, request_id_len, payload_len);
    memset(frame, 0, sizeof(frame));
    tcp_binary_encode_request_header(frame, &header);
    memcpy(frame + header_size, request_id, request_id_len);
    memcpy(frame + header_size + request_id_len, payload, payload_len);

    CHECK(tcp_binary_decode_request_frame(frame,
                                          header_size + request_id_len + payload_len,
                                          &decoded) == 1);
    CHECK(decoded.header.op == TCP_BINARY_OP_SQL);
    CHECK(memcmp(decoded.request_id, request_id, request_id_len) == 0);
    CHECK(memcmp(decoded.payload, payload, payload_len) == 0);
    return 0;
}

static int test_binary_header_rejects_invalid_values(void) {
    TCPBinaryRequestHeader request_header;
    TCPBinaryResponseHeader response_header;

    tcp_binary_init_request_header(&request_header, TCP_BINARY_OP_SQL, 4, 16);
    request_header.magic = 0;
    CHECK(tcp_binary_validate_request_header(&request_header) == 0);

    tcp_binary_init_response_header(&response_header,
                                    CMD_STATUS_OK,
                                    1,
                                    CMD_BODY_TEXT,
                                    4,
                                    8,
                                    0,
                                    0,
                                    0);
    response_header.version = 99;
    CHECK(tcp_binary_validate_response_header(&response_header) == 0);
    return 0;
}

static int decoded_request_id_equals(const TCPBinaryDecodedResponse *decoded,
                                     const char *expected) {
    size_t len;

    if (!decoded || !expected) return 0;
    len = strlen(expected);
    return decoded->header.request_id_len == len &&
           memcmp(decoded->request_id, expected, len) == 0;
}

static int read_response_frame(int fd,
                               unsigned char **out_frame,
                               size_t *out_frame_len,
                               TCPBinaryDecodedResponse *out_decoded) {
    TCPBinaryResponseHeader header;
    size_t header_size;
    size_t frame_len;
    unsigned char *frame;

    if (out_frame) *out_frame = NULL;
    if (out_frame_len) *out_frame_len = 0;
    if (!out_frame || !out_frame_len || !out_decoded) return -1;

    header_size = tcp_binary_response_header_size();
    frame = (unsigned char *)calloc(header_size, 1);
    if (!frame) return -1;
    if (recv_all_client(fd, frame, header_size) != 0) {
        free(frame);
        return -1;
    }
    CHECK(tcp_binary_decode_response_header(frame, header_size, &header) == 1);
    CHECK(tcp_binary_validate_response_header(&header) == 1);

    frame_len = tcp_binary_response_frame_size(&header);
    if (frame_len > header_size) {
        unsigned char *grown = (unsigned char *)realloc(frame, frame_len);
        CHECK(grown != NULL);
        frame = grown;
        CHECK(recv_all_client(fd, frame + header_size, frame_len - header_size) == 0);
    }
    CHECK(tcp_binary_decode_response_frame(frame, frame_len, out_decoded) == 1);
    *out_frame = frame;
    *out_frame_len = frame_len;
    return 0;
}

static int expect_response_status(int fd,
                                  const char *id,
                                  const char *status,
                                  int ok) {
    unsigned char *frame = NULL;
    size_t frame_len = 0;
    TCPBinaryDecodedResponse decoded;

    CHECK(read_response_frame(fd, &frame, &frame_len, &decoded) == 0);
    CHECK(decoded_request_id_equals(&decoded, id));
    CHECK(strcmp(cmd_status_to_string((CmdStatusCode)decoded.header.status), status) == 0);
    CHECK((decoded.header.flags & 1u) == (ok ? 1u : 0u));
    free(frame);
    return 0;
}

static int expect_two_ok_ids(int fd, const char *first_id, const char *second_id) {
    unsigned char *first_frame = NULL;
    unsigned char *second_frame = NULL;
    size_t first_len = 0;
    size_t second_len = 0;
    TCPBinaryDecodedResponse first;
    TCPBinaryDecodedResponse second;
    int saw_first;
    int saw_second;

    CHECK(read_response_frame(fd, &first_frame, &first_len, &first) == 0);
    CHECK(read_response_frame(fd, &second_frame, &second_len, &second) == 0);
    CHECK(first.header.status == CMD_STATUS_OK);
    CHECK(second.header.status == CMD_STATUS_OK);
    CHECK((first.header.flags & 1u) == 1u);
    CHECK((second.header.flags & 1u) == 1u);

    saw_first = decoded_request_id_equals(&first, first_id) ||
                decoded_request_id_equals(&second, first_id);
    saw_second = decoded_request_id_equals(&first, second_id) ||
                 decoded_request_id_equals(&second, second_id);
    CHECK(saw_first);
    CHECK(saw_second);

    free(first_frame);
    free(second_frame);
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
    unsigned char *frame = NULL;
    size_t frame_len = 0;
    TCPBinaryDecodedResponse decoded;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);
    CHECK(send_ping_request(fd, "p1") == 0);
    CHECK(read_response_frame(fd, &frame, &frame_len, &decoded) == 0);
    CHECK(decoded_request_id_equals(&decoded, "p1"));
    CHECK(decoded.header.status == CMD_STATUS_OK);
    CHECK((decoded.header.flags & 1u) == 1u);
    CHECK(decoded.header.body_format == CMD_BODY_TEXT);
    CHECK(decoded.header.body_len == 4);
    CHECK(memcmp(decoded.body, "pong", 4) == 0);
    free(frame);
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
    unsigned char *first_frame = NULL;
    unsigned char *second_frame = NULL;
    size_t first_len = 0;
    size_t second_len = 0;
    TCPBinaryDecodedResponse first;
    TCPBinaryDecodedResponse second;
    cJSON *body;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);
    CHECK(send_sql_request(fd, "s1", "SELECT 1;") == 0);
    CHECK(send_ping_request(fd, "p2") == 0);

    CHECK(read_response_frame(fd, &first_frame, &first_len, &first) == 0);
    CHECK(read_response_frame(fd, &second_frame, &second_len, &second) == 0);
    CHECK(decoded_request_id_equals(&first, "s1"));
    CHECK(first.header.status == CMD_STATUS_OK);
    CHECK(first.header.body_format == CMD_BODY_JSON);
    body = cJSON_ParseWithLength((const char *)first.body, first.header.body_len);
    CHECK(body != NULL);
    CHECK(cJSON_IsObject(body));
    {
        cJSON *sql = cJSON_GetObjectItemCaseSensitive(body, "sql");
        CHECK(cJSON_IsString(sql));
        CHECK(strcmp(sql->valuestring, "SELECT 1") == 0);
    }
    cJSON_Delete(body);
    CHECK(decoded_request_id_equals(&second, "p2"));
    CHECK(second.header.status == CMD_STATUS_OK);

    free(first_frame);
    free(second_frame);
    close(fd);
    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    return 0;
}

static int test_batched_requests_in_one_send(void) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port;
    int fd;
    unsigned char *batch = NULL;
    size_t batch_len = 0;
    const char *ids[] = { "b1", "b2", "b3" };
    int i;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    fd = connect_client(port);
    CHECK(fd >= 0);
    for (i = 0; i < 3; i++) {
        TCPBinaryRequestHeader header;
        size_t header_size = tcp_binary_request_header_size();
        size_t id_len = strlen(ids[i]);
        size_t frame_len;
        unsigned char *next;
        tcp_binary_init_request_header(&header, TCP_BINARY_OP_PING, id_len, 0);
        frame_len = tcp_binary_request_frame_size(&header);
        next = (unsigned char *)realloc(batch, batch_len + frame_len);
        CHECK(next != NULL);
        batch = next;
        tcp_binary_encode_request_header(batch + batch_len, &header);
        memcpy(batch + batch_len + header_size, ids[i], id_len);
        batch_len += frame_len;
    }
    CHECK(send_all_client(fd, (const char *)batch, batch_len) == 0);
    free(batch);
    CHECK(expect_response_status(fd, "b1", "OK", 1) == 0);
    CHECK(expect_response_status(fd, "b2", "OK", 1) == 0);
    CHECK(expect_response_status(fd, "b3", "OK", 1) == 0);

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

    {
        TCPBinaryRequestHeader bad_header;
        unsigned char bytes[32];
        tcp_binary_init_request_header(&bad_header, TCP_BINARY_OP_PING, 0, 0);
        bad_header.magic = 0;
        memset(bytes, 0, sizeof(bytes));
        tcp_binary_encode_request_header(bytes, &bad_header);
        CHECK(send_all_client(fd, (const char *)bytes, tcp_binary_request_header_size()) == 0);
    }
    CHECK(expect_response_status(fd, "unknown", "BAD_REQUEST", 0) == 0);
    CHECK(send_request_frame(fd, TCP_BINARY_OP_PING, "", NULL, 0) == 0);
    CHECK(expect_response_status(fd, "unknown", "BAD_REQUEST", 0) == 0);
    CHECK(send_request_frame(fd, (TCPBinaryOp)99, "u1", NULL, 0) == 0);
    CHECK(expect_response_status(fd, "unknown", "BAD_REQUEST", 0) == 0);
    CHECK(send_request_frame(fd, TCP_BINARY_OP_SQL, "s1", NULL, 0) == 0);
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
    unsigned char peek;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    first_fd = connect_client(port);
    second_fd = connect_client(port);
    CHECK(first_fd >= 0);
    CHECK(second_fd >= 0);

    CHECK(send_close_request(first_fd, "c1") == 0);
    CHECK(expect_response_status(first_fd, "c1", "OK", 1) == 0);
    CHECK(recv(first_fd, &peek, 1, 0) <= 0);

    CHECK(send_ping_request(second_fd, "p2") == 0);
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
    unsigned char peek;

    CHECK(start_mock_server(&processor, &server, &port) == 0);
    first_fd = connect_client(port);
    second_fd = connect_client(port);
    CHECK(first_fd >= 0);
    CHECK(second_fd >= 0);
    CHECK(send_ping_request(first_fd, "p1") == 0);
    CHECK(send_ping_request(second_fd, "p2") == 0);
    CHECK(expect_response_status(first_fd, "p1", "OK", 1) == 0);
    CHECK(expect_response_status(second_fd, "p2", "OK", 1) == 0);

    third_fd = connect_client(port);
    CHECK(third_fd >= 0);
    (void)send_ping_request(third_fd, "p3");
    CHECK(recv(third_fd, &peek, 1, 0) <= 0);

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
    unsigned char *first_frame = NULL;
    unsigned char *second_frame = NULL;
    size_t first_len = 0;
    size_t second_len = 0;
    TCPBinaryDecodedResponse first;
    TCPBinaryDecodedResponse second;

    CHECK(delayed_processor_create(&processor, &state) == 0);
    tcp_cmd_processor_config_init(&config, processor);
    CHECK(tcp_cmd_processor_start(&config, &server) == 0);
    port = tcp_cmd_processor_get_port(server);
    CHECK(port > 0);
    fd = connect_client(port);
    CHECK(fd >= 0);

    CHECK(send_sql_request(fd, "r1", "delay-first") == 0);
    CHECK(send_sql_request(fd, "r2", "fast-second") == 0);
    CHECK(delayed_wait_pending(state, 2));
    CHECK(send_sql_request(fd, "r3", "third") == 0);
    CHECK(expect_response_status(fd, "r3", "BUSY", 0) == 0);

    delayed_release_all(state);
    CHECK(read_response_frame(fd, &first_frame, &first_len, &first) == 0);
    CHECK(read_response_frame(fd, &second_frame, &second_len, &second) == 0);
    CHECK(decoded_request_id_equals(&first, "r2"));
    CHECK(decoded_request_id_equals(&second, "r1"));
    CHECK(first.header.status == CMD_STATUS_OK);
    CHECK(second.header.status == CMD_STATUS_OK);

    free(first_frame);
    free(second_frame);
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

    CHECK(send_sql_request(first_fd, "a1", "one") == 0);
    CHECK(send_sql_request(first_fd, "a2", "two") == 0);
    CHECK(delayed_wait_pending(state, 2));
    CHECK(send_sql_request(second_fd, "b1", "three") == 0);
    CHECK(delayed_wait_pending(state, 3));
    CHECK(send_sql_request(second_fd, "b2", "four") == 0);
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
    CHECK(test_binary_request_header_roundtrip() == 0);
    CHECK(test_binary_response_header_roundtrip() == 0);
    CHECK(test_binary_request_frame_decode() == 0);
    CHECK(test_binary_header_rejects_invalid_values() == 0);
    CHECK(test_start_port_and_ping() == 0);
    CHECK(test_sql_and_multiple_requests() == 0);
    CHECK(test_batched_requests_in_one_send() == 0);
    CHECK(test_invalid_requests() == 0);
    CHECK(test_close_only_current_connection() == 0);
    CHECK(test_connection_limit_per_client() == 0);
    CHECK(test_inflight_per_connection_limit_and_out_of_order() == 0);
    CHECK(test_inflight_per_client_limit() == 0);

    printf("[ok] tcp_cmd_processor checks passed\n");
    return 0;
}
