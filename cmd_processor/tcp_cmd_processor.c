#include "tcp_cmd_processor.h"

#include "../thirdparty/cjson/cJSON.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define TCP_CLIENT_KEY_BYTES 64

typedef struct TCPInflightId {
    char id[TCP_REQUEST_ID_MAX_BYTES + 1];
    struct TCPInflightId *next;
} TCPInflightId;

typedef struct TCPClientCounter {
    char key[TCP_CLIENT_KEY_BYTES];
    size_t connection_count;
    size_t inflight_count;
    struct TCPClientCounter *next;
} TCPClientCounter;

typedef struct TCPConnection {
    int client_fd;
    char client_key[TCP_CLIENT_KEY_BYTES];
    int closing;
    size_t inflight_count;
    size_t ref_count;
    pthread_t thread;
    pthread_mutex_t state_mutex;
    pthread_mutex_t write_mutex;
    TCPInflightId *inflight_ids;
    struct TCPCmdProcessor *server;
    struct TCPConnection *next;
} TCPConnection;

struct TCPCmdProcessor {
    int listen_fd;
    int actual_port;
    int stopping;
    int accept_thread_started;
    pthread_t accept_thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t active_clients;
    TCPConnection *connections;
    TCPClientCounter *client_counters;
    CmdProcessor *processor;
};

typedef enum {
    TCP_REQUEST_OP_SQL,
    TCP_REQUEST_OP_PING,
    TCP_REQUEST_OP_CLOSE
} TCPRequestOp;

static void connection_release(TCPConnection *connection);

static void copy_cstr(char *dst, size_t dst_size, const char *src) {
    size_t len;

    if (!dst || dst_size == 0) return;
    len = src ? strlen(src) : 0;
    if (len >= dst_size) len = dst_size - 1;
    if (len > 0) memcpy(dst, src, len);
    dst[len] = '\0';
}

static int set_socket_timeouts(int fd) {
    struct timeval read_timeout;
    struct timeval write_timeout;
    int tcp_nodelay = 1;
#ifdef SO_NOSIGPIPE
    int no_sigpipe = 1;
#endif

    read_timeout.tv_sec = TCP_READ_TIMEOUT_MS / 1000;
    read_timeout.tv_usec = (TCP_READ_TIMEOUT_MS % 1000) * 1000;
    write_timeout.tv_sec = TCP_WRITE_TIMEOUT_MS / 1000;
    write_timeout.tv_usec = (TCP_WRITE_TIMEOUT_MS % 1000) * 1000;

    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &read_timeout, sizeof(read_timeout)) != 0) {
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &write_timeout, sizeof(write_timeout)) != 0) {
        return -1;
    }
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay));
#ifdef SO_NOSIGPIPE
    (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#endif
    return 0;
}

static TCPClientCounter *find_client_counter(TCPCmdProcessor *server,
                                             const char *client_key) {
    TCPClientCounter *counter;

    if (!server || !client_key) return NULL;
    counter = server->client_counters;
    while (counter) {
        if (strcmp(counter->key, client_key) == 0) return counter;
        counter = counter->next;
    }
    return NULL;
}

static TCPClientCounter *get_or_create_client_counter(TCPCmdProcessor *server,
                                                      const char *client_key) {
    TCPClientCounter *counter;

    counter = find_client_counter(server, client_key);
    if (counter) return counter;

    counter = (TCPClientCounter *)calloc(1, sizeof(*counter));
    if (!counter) return NULL;
    copy_cstr(counter->key, sizeof(counter->key), client_key);
    counter->next = server->client_counters;
    server->client_counters = counter;
    return counter;
}

static void remove_client_counter_if_empty(TCPCmdProcessor *server,
                                           TCPClientCounter *target) {
    TCPClientCounter **cursor;

    if (!server || !target) return;
    if (target->connection_count != 0 || target->inflight_count != 0) return;

    cursor = &server->client_counters;
    while (*cursor) {
        if (*cursor == target) {
            *cursor = target->next;
            free(target);
            return;
        }
        cursor = &(*cursor)->next;
    }
}

static int reserve_connection_slot(TCPCmdProcessor *server,
                                   const char *client_key) {
    TCPClientCounter *counter;
    int ok = 0;

    pthread_mutex_lock(&server->mutex);
    if (!server->stopping &&
        server->active_clients < TCP_MAX_CONNECTIONS_TOTAL) {
        counter = get_or_create_client_counter(server, client_key);
        if (counter && counter->connection_count < TCP_MAX_CONNECTIONS_PER_CLIENT) {
            counter->connection_count++;
            server->active_clients++;
            ok = 1;
        } else if (counter) {
            remove_client_counter_if_empty(server, counter);
        }
    }
    pthread_mutex_unlock(&server->mutex);
    return ok;
}

static void release_connection_slot(TCPCmdProcessor *server,
                                    const char *client_key) {
    TCPClientCounter *counter;

    pthread_mutex_lock(&server->mutex);
    counter = find_client_counter(server, client_key);
    if (counter && counter->connection_count > 0) counter->connection_count--;
    if (server->active_clients > 0) server->active_clients--;
    if (counter) remove_client_counter_if_empty(server, counter);
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static int connection_add_ref(TCPConnection *connection) {
    int ok = 0;

    if (!connection) return 0;
    pthread_mutex_lock(&connection->state_mutex);
    if (connection->ref_count > 0) {
        connection->ref_count++;
        ok = 1;
    }
    pthread_mutex_unlock(&connection->state_mutex);
    return ok;
}

static void connection_close_fd(TCPConnection *connection) {
    int fd = -1;

    if (!connection) return;
    pthread_mutex_lock(&connection->write_mutex);
    pthread_mutex_lock(&connection->state_mutex);
    connection->closing = 1;
    if (connection->client_fd >= 0) {
        fd = connection->client_fd;
        connection->client_fd = -1;
    }
    pthread_mutex_unlock(&connection->state_mutex);
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
    pthread_mutex_unlock(&connection->write_mutex);
}

static int connection_is_closing(TCPConnection *connection) {
    int closing;

    pthread_mutex_lock(&connection->state_mutex);
    closing = connection->closing;
    pthread_mutex_unlock(&connection->state_mutex);
    return closing;
}

static void remove_connection_from_server(TCPConnection *connection) {
    TCPCmdProcessor *server;
    TCPConnection **cursor;

    server = connection->server;
    pthread_mutex_lock(&server->mutex);
    cursor = &server->connections;
    while (*cursor) {
        if (*cursor == connection) {
            *cursor = connection->next;
            break;
        }
        cursor = &(*cursor)->next;
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static void free_inflight_list(TCPConnection *connection) {
    TCPInflightId *node;
    TCPInflightId *next;

    node = connection->inflight_ids;
    while (node) {
        next = node->next;
        free(node);
        node = next;
    }
    connection->inflight_ids = NULL;
}

static void connection_destroy(TCPConnection *connection) {
    if (!connection) return;

    connection_close_fd(connection);
    remove_connection_from_server(connection);
    release_connection_slot(connection->server, connection->client_key);

    free_inflight_list(connection);
    pthread_mutex_destroy(&connection->state_mutex);
    pthread_mutex_destroy(&connection->write_mutex);
    free(connection);
}

static void connection_release(TCPConnection *connection) {
    int should_destroy = 0;

    if (!connection) return;
    pthread_mutex_lock(&connection->state_mutex);
    if (connection->ref_count > 0) {
        connection->ref_count--;
        should_destroy = connection->ref_count == 0;
    }
    pthread_mutex_unlock(&connection->state_mutex);

    if (should_destroy) connection_destroy(connection);
}

static int inflight_contains(TCPConnection *connection, const char *request_id) {
    TCPInflightId *node;

    node = connection->inflight_ids;
    while (node) {
        if (strcmp(node->id, request_id) == 0) return 1;
        node = node->next;
    }
    return 0;
}

static int register_inflight(TCPConnection *connection,
                             const char *request_id,
                             CmdStatusCode *out_status,
                             const char **out_message) {
    TCPCmdProcessor *server;
    TCPClientCounter *counter;
    TCPInflightId *node;
    int ok = 0;

    if (out_status) *out_status = CMD_STATUS_INTERNAL_ERROR;
    if (out_message) *out_message = "failed to register request";

    node = (TCPInflightId *)calloc(1, sizeof(*node));
    if (!node) return 0;
    copy_cstr(node->id, sizeof(node->id), request_id);

    server = connection->server;
    pthread_mutex_lock(&server->mutex);
    pthread_mutex_lock(&connection->state_mutex);

    counter = find_client_counter(server, connection->client_key);
    if (connection->closing) {
        if (out_status) *out_status = CMD_STATUS_BUSY;
        if (out_message) *out_message = "connection is closing";
    } else if (inflight_contains(connection, request_id)) {
        if (out_status) *out_status = CMD_STATUS_BAD_REQUEST;
        if (out_message) *out_message = "duplicate in-flight request id";
    } else if (connection->inflight_count >= TCP_MAX_INFLIGHT_PER_CONNECTION) {
        if (out_status) *out_status = CMD_STATUS_BUSY;
        if (out_message) *out_message = "too many in-flight requests on connection";
    } else if (!counter || counter->inflight_count >= TCP_MAX_INFLIGHT_PER_CLIENT) {
        if (out_status) *out_status = CMD_STATUS_BUSY;
        if (out_message) *out_message = "too many in-flight requests for client";
    } else {
        node->next = connection->inflight_ids;
        connection->inflight_ids = node;
        connection->inflight_count++;
        counter->inflight_count++;
        ok = 1;
    }

    pthread_mutex_unlock(&connection->state_mutex);
    pthread_mutex_unlock(&server->mutex);

    if (!ok) free(node);
    return ok;
}

static void remove_inflight(TCPConnection *connection, const char *request_id) {
    TCPCmdProcessor *server;
    TCPClientCounter *counter;
    TCPInflightId **cursor;
    TCPInflightId *removed = NULL;

    if (!connection || !request_id) return;

    server = connection->server;
    pthread_mutex_lock(&server->mutex);
    pthread_mutex_lock(&connection->state_mutex);

    cursor = &connection->inflight_ids;
    while (*cursor) {
        if (strcmp((*cursor)->id, request_id) == 0) {
            removed = *cursor;
            *cursor = removed->next;
            if (connection->inflight_count > 0) connection->inflight_count--;
            counter = find_client_counter(server, connection->client_key);
            if (counter && counter->inflight_count > 0) counter->inflight_count--;
            if (counter) remove_client_counter_if_empty(server, counter);
            break;
        }
        cursor = &(*cursor)->next;
    }

    pthread_mutex_unlock(&connection->state_mutex);
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
    free(removed);
}

static int send_all(int fd, const char *data, size_t len) {
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

static int write_json_line(TCPConnection *connection, const char *json) {
    int fd = -1;
    int rc = -1;
    size_t json_len;
    char *line = NULL;

    if (!connection || !json) return -1;
    json_len = strlen(json);

    pthread_mutex_lock(&connection->write_mutex);
    pthread_mutex_lock(&connection->state_mutex);
    if (!connection->closing && connection->client_fd >= 0) fd = connection->client_fd;
    pthread_mutex_unlock(&connection->state_mutex);

    if (fd >= 0) {
        line = (char *)malloc(json_len + 2);
        if (line) {
            memcpy(line, json, json_len);
            line[json_len] = '\n';
            line[json_len + 1] = '\0';
            if (send_all(fd, line, json_len + 1) == 0) rc = 0;
        }
    }

    if (rc != 0) {
        int close_fd = -1;
        pthread_mutex_lock(&connection->state_mutex);
        connection->closing = 1;
        if (connection->client_fd >= 0) {
            close_fd = connection->client_fd;
            connection->client_fd = -1;
        }
        pthread_mutex_unlock(&connection->state_mutex);
        if (close_fd >= 0) {
            shutdown(close_fd, SHUT_RDWR);
            close(close_fd);
        }
    }

    free(line);
    pthread_mutex_unlock(&connection->write_mutex);
    return rc;
}

static char *build_status_json(const char *request_id,
                               CmdStatusCode status,
                               const char *message) {
    cJSON *root;
    char *json;
    int ok;
    char error_message[TCP_ERROR_MESSAGE_MAX_BYTES + 1];

    root = cJSON_CreateObject();
    if (!root) return NULL;

    ok = status == CMD_STATUS_OK;
    cJSON_AddStringToObject(root, "id", request_id ? request_id : "unknown");
    cJSON_AddBoolToObject(root, "ok", ok ? 1 : 0);
    cJSON_AddStringToObject(root, "status", cmd_status_to_string(status));
    if (!ok) {
        copy_cstr(error_message, sizeof(error_message), message ? message : "request failed");
        cJSON_AddStringToObject(root, "error", error_message);
    }

    json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return json;
}

static int write_status_response(TCPConnection *connection,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 const char *message) {
    char *json;
    int rc = -1;

    json = build_status_json(request_id, status, message);
    if (json) {
        rc = write_json_line(connection, json);
        cJSON_free(json);
    }
    return rc;
}

static char *build_cmd_response_json(CmdResponse *response) {
    cJSON *root;
    cJSON *body_json;
    char *json;

    if (!response) return NULL;

    root = cJSON_CreateObject();
    if (!root) return NULL;

    cJSON_AddStringToObject(root, "id", response->request_id);
    cJSON_AddBoolToObject(root, "ok", response->ok ? 1 : 0);
    cJSON_AddStringToObject(root, "status", cmd_status_to_string(response->status));
    if (response->row_count != 0) cJSON_AddNumberToObject(root, "row_count", response->row_count);
    if (response->affected_count != 0) {
        cJSON_AddNumberToObject(root, "affected_count", response->affected_count);
    }

    if (response->body && response->body_len > 0) {
        if (response->body_format == CMD_BODY_TEXT) {
            cJSON_AddStringToObject(root, "body", response->body);
        } else if (response->body_format == CMD_BODY_JSON) {
            body_json = cJSON_ParseWithLength(response->body, response->body_len);
            if (!body_json) {
                cJSON_Delete(root);
                return NULL;
            }
            cJSON_AddItemToObject(root, "body", body_json);
        }
    }

    if (!response->ok && response->error_message) {
        cJSON_AddStringToObject(root, "error", response->error_message);
    }

    json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return json;
}

static void tcp_response_callback(CmdProcessor *processor,
                                  CmdRequest *request,
                                  CmdResponse *response,
                                  void *user_data) {
    TCPConnection *connection;
    const char *request_id;
    char *json;

    connection = (TCPConnection *)user_data;
    request_id = request ? request->request_id : (response ? response->request_id : "unknown");

    if (response) {
        json = build_cmd_response_json(response);
        if (json) {
            (void)write_json_line(connection, json);
            cJSON_free(json);
        } else {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_INTERNAL_ERROR,
                                        "response serialization failed");
        }
    } else {
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_INTERNAL_ERROR,
                                    "processor returned no response");
    }

    remove_inflight(connection, request_id);
    if (response) cmd_processor_release_response(processor, response);
    if (request) cmd_processor_release_request(processor, request);
    connection_release(connection);
}

static int read_json_line(TCPConnection *connection,
                          char *buffer,
                          size_t buffer_size,
                          size_t *out_len,
                          int *out_too_long) {
    size_t len = 0;
    int too_long = 0;

    if (out_len) *out_len = 0;
    if (out_too_long) *out_too_long = 0;
    if (!connection || !buffer || buffer_size == 0) return -1;

    while (!connection_is_closing(connection)) {
        char ch;
        ssize_t n;
        int fd;

        pthread_mutex_lock(&connection->state_mutex);
        fd = connection->client_fd;
        pthread_mutex_unlock(&connection->state_mutex);
        if (fd < 0) return -1;

        n = recv(fd, &ch, 1, 0);
        if (n == 0) return -1;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }

        if (ch == '\n') {
            if (len > 0 && buffer[len - 1] == '\r') len--;
            buffer[len] = '\0';
            if (out_len) *out_len = len;
            if (out_too_long) *out_too_long = too_long;
            return too_long ? 1 : 0;
        }

        if (!too_long) {
            if (len + 1 < buffer_size) {
                buffer[len++] = ch;
            } else {
                too_long = 1;
            }
        }
    }

    return -1;
}

static int get_string_field(cJSON *root, const char *name, const char **out_value) {
    cJSON *item;

    if (out_value) *out_value = NULL;
    item = cJSON_GetObjectItemCaseSensitive(root, name);
    if (!cJSON_IsString(item) || !item->valuestring) return 0;
    if (out_value) *out_value = item->valuestring;
    return 1;
}

static int parse_op(const char *op_value, TCPRequestOp *out_op) {
    if (strcmp(op_value, "sql") == 0) {
        *out_op = TCP_REQUEST_OP_SQL;
        return 1;
    }
    if (strcmp(op_value, "ping") == 0) {
        *out_op = TCP_REQUEST_OP_PING;
        return 1;
    }
    if (strcmp(op_value, "close") == 0) {
        *out_op = TCP_REQUEST_OP_CLOSE;
        return 1;
    }
    return 0;
}

static void submit_parsed_request(TCPConnection *connection,
                                  const char *request_id,
                                  TCPRequestOp op,
                                  const char *sql) {
    CmdProcessor *processor;
    CmdRequest *cmd_request = NULL;
    CmdStatusCode status;
    const char *message;
    int submit_rc;

    processor = connection->server->processor;
    if (!register_inflight(connection, request_id, &status, &message)) {
        (void)write_status_response(connection, request_id, status, message);
        return;
    }

    if (cmd_processor_acquire_request(processor, &cmd_request) != 0) {
        remove_inflight(connection, request_id);
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_INTERNAL_ERROR,
                                    "request slot unavailable");
        return;
    }

    if (op == TCP_REQUEST_OP_SQL) {
        status = cmd_processor_set_sql_request(processor, cmd_request, request_id, sql);
    } else {
        status = cmd_processor_set_ping_request(processor, cmd_request, request_id);
    }

    if (status != CMD_STATUS_OK) {
        cmd_processor_release_request(processor, cmd_request);
        remove_inflight(connection, request_id);
        (void)write_status_response(connection, request_id, status, "invalid request");
        return;
    }

    if (!connection_add_ref(connection)) {
        cmd_processor_release_request(processor, cmd_request);
        remove_inflight(connection, request_id);
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_INTERNAL_ERROR,
                                    "connection unavailable");
        return;
    }

    submit_rc = cmd_processor_submit(processor,
                                     cmd_request,
                                     tcp_response_callback,
                                     connection);
    if (submit_rc != 0) {
        remove_inflight(connection, request_id);
        cmd_processor_release_request(processor, cmd_request);
        connection_release(connection);
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_INTERNAL_ERROR,
                                    "submit failed");
    }
}

static void process_json_line(TCPConnection *connection,
                              const char *line,
                              size_t line_len) {
    cJSON *root;
    const char *request_id;
    const char *op_value;
    const char *sql_value = NULL;
    TCPRequestOp op;

    root = cJSON_ParseWithLength(line, line_len);
    if (!root || !cJSON_IsObject(root)) {
        if (root) cJSON_Delete(root);
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "invalid JSON object");
        return;
    }

    if (!get_string_field(root, "id", &request_id)) {
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "missing id");
        cJSON_Delete(root);
        return;
    }
    if (request_id[0] == '\0') {
        (void)write_status_response(connection,
                                    "",
                                    CMD_STATUS_BAD_REQUEST,
                                    "empty id");
        cJSON_Delete(root);
        return;
    }
    if (strlen(request_id) > TCP_REQUEST_ID_MAX_BYTES) {
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "id too long");
        cJSON_Delete(root);
        return;
    }

    if (!get_string_field(root, "op", &op_value)) {
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_BAD_REQUEST,
                                    "missing op");
        cJSON_Delete(root);
        return;
    }
    if (strlen(op_value) > TCP_OP_MAX_BYTES || !parse_op(op_value, &op)) {
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_BAD_REQUEST,
                                    "unknown op");
        cJSON_Delete(root);
        return;
    }

    if (op == TCP_REQUEST_OP_CLOSE) {
        (void)write_status_response(connection, request_id, CMD_STATUS_OK, NULL);
        cJSON_Delete(root);
        connection_close_fd(connection);
        return;
    }

    if (op == TCP_REQUEST_OP_SQL) {
        if (!get_string_field(root, "sql", &sql_value)) {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_BAD_REQUEST,
                                        "missing sql");
            cJSON_Delete(root);
            return;
        }
        if (strlen(sql_value) > connection->server->processor->context->max_sql_len) {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_SQL_TOO_LONG,
                                        "sql too long");
            cJSON_Delete(root);
            return;
        }
    }

    submit_parsed_request(connection, request_id, op, sql_value);
    cJSON_Delete(root);
}

static void *connection_thread_main(void *arg) {
    TCPConnection *connection;
    char *line;
    size_t line_len;
    int too_long;

    connection = (TCPConnection *)arg;
    line = (char *)calloc(TCP_MAX_LINE_BYTES + 1, 1);
    if (!line) {
        connection_close_fd(connection);
        connection_release(connection);
        return NULL;
    }

    while (!connection_is_closing(connection)) {
        int rc = read_json_line(connection,
                                line,
                                TCP_MAX_LINE_BYTES + 1,
                                &line_len,
                                &too_long);
        if (rc < 0) break;
        if (too_long) {
            (void)write_status_response(connection,
                                        "unknown",
                                        CMD_STATUS_BAD_REQUEST,
                                        "line too long");
            continue;
        }
        process_json_line(connection, line, line_len);
    }

    free(line);
    connection_close_fd(connection);
    connection_release(connection);
    return NULL;
}

static void make_client_key(const struct sockaddr_storage *addr,
                            char *dst,
                            size_t dst_size) {
    const void *source = NULL;

    if (!addr || !dst || dst_size == 0) return;
    dst[0] = '\0';
    if (addr->ss_family == AF_INET) {
        source = &((const struct sockaddr_in *)addr)->sin_addr;
    } else if (addr->ss_family == AF_INET6) {
        source = &((const struct sockaddr_in6 *)addr)->sin6_addr;
    }
    if (!source || !inet_ntop(addr->ss_family, source, dst, (socklen_t)dst_size)) {
        copy_cstr(dst, dst_size, "unknown");
    }
}

static TCPConnection *create_connection(TCPCmdProcessor *server,
                                        int client_fd,
                                        const char *client_key) {
    TCPConnection *connection;

    connection = (TCPConnection *)calloc(1, sizeof(*connection));
    if (!connection) return NULL;

    connection->client_fd = client_fd;
    connection->server = server;
    connection->ref_count = 1;
    copy_cstr(connection->client_key, sizeof(connection->client_key), client_key);
    if (pthread_mutex_init(&connection->state_mutex, NULL) != 0) {
        free(connection);
        return NULL;
    }
    if (pthread_mutex_init(&connection->write_mutex, NULL) != 0) {
        pthread_mutex_destroy(&connection->state_mutex);
        free(connection);
        return NULL;
    }

    pthread_mutex_lock(&server->mutex);
    connection->next = server->connections;
    server->connections = connection;
    pthread_mutex_unlock(&server->mutex);

    return connection;
}

static void *accept_thread_main(void *arg) {
    TCPCmdProcessor *server;

    server = (TCPCmdProcessor *)arg;
    while (1) {
        struct sockaddr_storage client_addr;
        socklen_t addr_len = sizeof(client_addr);
        char client_key[TCP_CLIENT_KEY_BYTES];
        TCPConnection *connection;
        int client_fd;

        pthread_mutex_lock(&server->mutex);
        if (server->stopping) {
            pthread_mutex_unlock(&server->mutex);
            break;
        }
        pthread_mutex_unlock(&server->mutex);

        client_fd = accept(server->listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            pthread_mutex_lock(&server->mutex);
            if (server->stopping) {
                pthread_mutex_unlock(&server->mutex);
                break;
            }
            pthread_mutex_unlock(&server->mutex);
            continue;
        }

        make_client_key(&client_addr, client_key, sizeof(client_key));
        if (!reserve_connection_slot(server, client_key)) {
            close(client_fd);
            continue;
        }
        if (set_socket_timeouts(client_fd) != 0) {
            close(client_fd);
            release_connection_slot(server, client_key);
            continue;
        }

        connection = create_connection(server, client_fd, client_key);
        if (!connection) {
            close(client_fd);
            release_connection_slot(server, client_key);
            continue;
        }

        if (pthread_create(&connection->thread, NULL, connection_thread_main, connection) != 0) {
            connection_close_fd(connection);
            connection_release(connection);
            continue;
        }
        pthread_detach(connection->thread);
    }

    return NULL;
}

static int create_listen_socket(const TCPCmdProcessorConfig *config, int *out_fd, int *out_port) {
    int fd;
    int reuse = 1;
    struct sockaddr_in addr;
    socklen_t addr_len;

    if (out_fd) *out_fd = -1;
    if (out_port) *out_port = 0;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
        close(fd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)config->port);
    if (inet_pton(AF_INET, config->host, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, config->backlog) != 0) {
        close(fd);
        return -1;
    }

    addr_len = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0) {
        close(fd);
        return -1;
    }

    if (out_fd) *out_fd = fd;
    if (out_port) *out_port = ntohs(addr.sin_port);
    return 0;
}

static int validate_config(const TCPCmdProcessorConfig *config) {
    if (!config || !config->processor || !config->processor->context) return 0;
    if (!config->host || config->host[0] == '\0') return 0;
    if (config->port < 0 || config->port > 65535) return 0;
    if (config->backlog <= 0) return 0;
    if (TCP_MAX_CONNECTIONS_TOTAL <= 0 ||
        TCP_MAX_CONNECTIONS_PER_CLIENT <= 0 ||
        TCP_MAX_INFLIGHT_PER_CONNECTION <= 0 ||
        TCP_MAX_INFLIGHT_PER_CLIENT <= 0 ||
        TCP_MAX_LINE_BYTES <= 0 ||
        TCP_REQUEST_ID_MAX_BYTES <= 0 ||
        TCP_OP_MAX_BYTES <= 0 ||
        TCP_ERROR_MESSAGE_MAX_BYTES <= 0) {
        return 0;
    }
    if (TCP_REQUEST_ID_MAX_BYTES >= (int)sizeof(((CmdRequest *)0)->request_id)) return 0;
    if (TCP_MAX_CONNECTIONS_PER_CLIENT > TCP_MAX_CONNECTIONS_TOTAL) return 0;
    if (TCP_MAX_INFLIGHT_PER_CONNECTION > TCP_MAX_INFLIGHT_PER_CLIENT) return 0;
    if (TCP_MAX_LINE_BYTES < config->processor->context->max_sql_len) return 0;
    return 1;
}

void tcp_cmd_processor_config_init(TCPCmdProcessorConfig *config,
                                   CmdProcessor *processor) {
    if (!config) return;
    memset(config, 0, sizeof(*config));
    config->host = TCP_DEFAULT_HOST;
    config->port = TCP_DEFAULT_PORT;
    config->backlog = TCP_DEFAULT_BACKLOG;
    config->processor = processor;
}

int tcp_cmd_processor_start(const TCPCmdProcessorConfig *config,
                            TCPCmdProcessor **out_server) {
    TCPCmdProcessor *server;
    int listen_fd = -1;
    int actual_port = 0;

    if (out_server) *out_server = NULL;
    if (!out_server || !validate_config(config)) return -1;
    if (create_listen_socket(config, &listen_fd, &actual_port) != 0) return -1;

    server = (TCPCmdProcessor *)calloc(1, sizeof(*server));
    if (!server) {
        close(listen_fd);
        return -1;
    }

    server->listen_fd = listen_fd;
    server->actual_port = actual_port;
    server->processor = config->processor;
    if (pthread_mutex_init(&server->mutex, NULL) != 0) {
        close(listen_fd);
        free(server);
        return -1;
    }
    if (pthread_cond_init(&server->cond, NULL) != 0) {
        pthread_mutex_destroy(&server->mutex);
        close(listen_fd);
        free(server);
        return -1;
    }

    if (pthread_create(&server->accept_thread, NULL, accept_thread_main, server) != 0) {
        pthread_cond_destroy(&server->cond);
        pthread_mutex_destroy(&server->mutex);
        close(listen_fd);
        free(server);
        return -1;
    }
    server->accept_thread_started = 1;

    *out_server = server;
    return 0;
}

int tcp_cmd_processor_get_port(TCPCmdProcessor *server) {
    if (!server) return -1;
    return server->actual_port;
}

static void close_listen_socket(TCPCmdProcessor *server) {
    int fd = -1;

    pthread_mutex_lock(&server->mutex);
    if (server->listen_fd >= 0) {
        fd = server->listen_fd;
        server->listen_fd = -1;
    }
    pthread_mutex_unlock(&server->mutex);

    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
}

static void close_all_connections(TCPCmdProcessor *server) {
    TCPConnection *items[TCP_MAX_CONNECTIONS_TOTAL];
    size_t count = 0;
    size_t i;
    TCPConnection *connection;

    pthread_mutex_lock(&server->mutex);
    connection = server->connections;
    while (connection && count < TCP_MAX_CONNECTIONS_TOTAL) {
        if (connection_add_ref(connection)) {
            items[count++] = connection;
        }
        connection = connection->next;
    }
    pthread_mutex_unlock(&server->mutex);

    for (i = 0; i < count; i++) {
        connection_close_fd(items[i]);
        connection_release(items[i]);
    }
}

void tcp_cmd_processor_stop(TCPCmdProcessor *server) {
    if (!server) return;

    pthread_mutex_lock(&server->mutex);
    server->stopping = 1;
    pthread_mutex_unlock(&server->mutex);

    close_listen_socket(server);
    if (server->accept_thread_started) {
        pthread_join(server->accept_thread, NULL);
        server->accept_thread_started = 0;
    }

    close_all_connections(server);

    pthread_mutex_lock(&server->mutex);
    while (server->connections != NULL) {
        pthread_cond_wait(&server->cond, &server->mutex);
    }
    pthread_mutex_unlock(&server->mutex);

    pthread_cond_destroy(&server->cond);
    pthread_mutex_destroy(&server->mutex);
    free(server);
}
