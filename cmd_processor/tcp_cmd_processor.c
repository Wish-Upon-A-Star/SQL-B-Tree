#include "tcp_cmd_processor.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
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

#ifndef TCP_READ_BUFFER_BYTES
#define TCP_READ_BUFFER_BYTES 65536
#endif

#ifndef TCP_JSON_INLINE_BYTES
#define TCP_JSON_INLINE_BYTES 16384
#endif

#ifndef TCP_SOCKET_BUFFER_BYTES
#define TCP_SOCKET_BUFFER_BYTES 1048576
#endif

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
    size_t read_start;
    size_t read_end;
    char read_buffer[TCP_READ_BUFFER_BYTES];
    pthread_t thread;
    pthread_mutex_t state_mutex;
    pthread_mutex_t write_mutex;
    TCPInflightId *inflight_ids;
    TCPInflightId *free_inflight_ids;
    TCPInflightId inflight_pool[TCP_MAX_INFLIGHT_PER_CONNECTION];
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
    int socket_buffer = TCP_SOCKET_BUFFER_BYTES;
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
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &socket_buffer, sizeof(socket_buffer));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &socket_buffer, sizeof(socket_buffer));
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
    connection->inflight_ids = NULL;
    connection->free_inflight_ids = NULL;
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

static void init_inflight_pool(TCPConnection *connection) {
    int i;

    if (!connection) return;
    connection->free_inflight_ids = NULL;
    for (i = 0; i < TCP_MAX_INFLIGHT_PER_CONNECTION; i++) {
        connection->inflight_pool[i].id[0] = '\0';
        connection->inflight_pool[i].next = connection->free_inflight_ids;
        connection->free_inflight_ids = &connection->inflight_pool[i];
    }
}

static TCPInflightId *acquire_inflight_node(TCPConnection *connection) {
    TCPInflightId *node;

    if (!connection || !connection->free_inflight_ids) return NULL;
    node = connection->free_inflight_ids;
    connection->free_inflight_ids = node->next;
    node->next = NULL;
    node->id[0] = '\0';
    return node;
}

static void release_inflight_node(TCPConnection *connection, TCPInflightId *node) {
    if (!connection || !node) return;
    node->id[0] = '\0';
    node->next = connection->free_inflight_ids;
    connection->free_inflight_ids = node;
}

static int register_inflight(TCPConnection *connection,
                             const char *request_id,
                             CmdStatusCode *out_status,
                             const char **out_message) {
    TCPCmdProcessor *server;
    TCPClientCounter *counter;
    TCPInflightId *node = NULL;
    int ok = 0;

    if (out_status) *out_status = CMD_STATUS_INTERNAL_ERROR;
    if (out_message) *out_message = "failed to register request";

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
        node = acquire_inflight_node(connection);
        if (!node) {
            if (out_status) *out_status = CMD_STATUS_INTERNAL_ERROR;
            if (out_message) *out_message = "in-flight request pool exhausted";
            goto unlock;
        }
        copy_cstr(node->id, sizeof(node->id), request_id);
        node->next = connection->inflight_ids;
        connection->inflight_ids = node;
        connection->inflight_count++;
        counter->inflight_count++;
        ok = 1;
    }

unlock:
    pthread_mutex_unlock(&connection->state_mutex);
    pthread_mutex_unlock(&server->mutex);

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
            release_inflight_node(connection, removed);
            break;
        }
        cursor = &(*cursor)->next;
    }

    pthread_mutex_unlock(&connection->state_mutex);
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
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

static int write_response_bytes(TCPConnection *connection, const char *data, size_t len) {
    int fd = -1;
    int rc = -1;

    if (!connection || (!data && len != 0)) return -1;

    pthread_mutex_lock(&connection->write_mutex);
    pthread_mutex_lock(&connection->state_mutex);
    if (!connection->closing && connection->client_fd >= 0) fd = connection->client_fd;
    pthread_mutex_unlock(&connection->state_mutex);

    if (fd >= 0) {
        if (send_all(fd, data, len) == 0) rc = 0;
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

    pthread_mutex_unlock(&connection->write_mutex);
    return rc;
}

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    char inline_data[TCP_JSON_INLINE_BYTES];
} JSONBuffer;

static void json_buffer_init(JSONBuffer *buffer) {
    if (!buffer) return;
    buffer->data = buffer->inline_data;
    buffer->len = 0;
    buffer->cap = sizeof(buffer->inline_data);
    buffer->inline_data[0] = '\0';
}

static void json_buffer_destroy(JSONBuffer *buffer) {
    if (!buffer) return;
    if (buffer->data && buffer->data != buffer->inline_data) free(buffer->data);
    buffer->data = buffer->inline_data;
    buffer->len = 0;
    buffer->cap = sizeof(buffer->inline_data);
    buffer->inline_data[0] = '\0';
}

static int json_buffer_reserve(JSONBuffer *buffer, size_t extra) {
    char *next;
    size_t required;
    size_t cap;

    if (!buffer) return 0;
    if (!buffer->data) json_buffer_init(buffer);
    if (extra > ((size_t)-1) - buffer->len - 1) return 0;
    required = buffer->len + extra + 1;
    if (required <= buffer->cap) return 1;

    cap = buffer->cap ? buffer->cap : 256;
    while (cap < required) {
        if (cap > ((size_t)-1) / 2) {
            cap = required;
            break;
        }
        cap *= 2;
    }

    if (buffer->data == buffer->inline_data) {
        next = (char *)malloc(cap);
        if (next && buffer->len > 0) memcpy(next, buffer->data, buffer->len);
        if (next) next[buffer->len] = '\0';
    } else {
        next = (char *)realloc(buffer->data, cap);
    }
    if (!next) return 0;
    buffer->data = next;
    buffer->cap = cap;
    return 1;
}

static int json_buffer_append_bytes(JSONBuffer *buffer, const char *data, size_t len) {
    if (!buffer || (!data && len != 0)) return 0;
    if (!json_buffer_reserve(buffer, len)) return 0;
    if (len > 0) memcpy(buffer->data + buffer->len, data, len);
    buffer->len += len;
    buffer->data[buffer->len] = '\0';
    return 1;
}

static int json_buffer_append_cstr(JSONBuffer *buffer, const char *text) {
    return json_buffer_append_bytes(buffer, text, text ? strlen(text) : 0);
}

static int json_buffer_append_char(JSONBuffer *buffer, char ch) {
    return json_buffer_append_bytes(buffer, &ch, 1);
}

static int json_buffer_append_int(JSONBuffer *buffer, int value) {
    char scratch[32];
    int len;

    len = snprintf(scratch, sizeof(scratch), "%d", value);
    if (len < 0) return 0;
    return json_buffer_append_bytes(buffer, scratch, (size_t)len);
}

static int json_buffer_append_escaped(JSONBuffer *buffer, const char *text, size_t text_len) {
    size_t i;
    size_t chunk_start = 0;

    if (!json_buffer_append_char(buffer, '"')) return 0;
    for (i = 0; i < text_len; i++) {
        unsigned char ch = (unsigned char)text[i];
        const char *escape = NULL;
        char unicode_escape[7];
        int unicode_len = 0;

        switch (ch) {
            case '\\':
                escape = "\\\\";
                break;
            case '"':
                escape = "\\\"";
                break;
            case '\b':
                escape = "\\b";
                break;
            case '\f':
                escape = "\\f";
                break;
            case '\n':
                escape = "\\n";
                break;
            case '\r':
                escape = "\\r";
                break;
            case '\t':
                escape = "\\t";
                break;
            default:
                if (ch < 0x20) {
                    unicode_len = snprintf(unicode_escape, sizeof(unicode_escape), "\\u%04x", ch);
                    if (unicode_len < 0) return 0;
                }
                break;
        }

        if (!escape && unicode_len == 0) continue;

        if (i > chunk_start &&
            !json_buffer_append_bytes(buffer, text + chunk_start, i - chunk_start)) {
            return 0;
        }
        if (escape) {
            if (!json_buffer_append_cstr(buffer, escape)) return 0;
        } else if (!json_buffer_append_bytes(buffer,
                                            unicode_escape,
                                            (size_t)unicode_len)) {
            return 0;
        }
        chunk_start = i + 1;
    }
    if (i > chunk_start &&
        !json_buffer_append_bytes(buffer, text + chunk_start, i - chunk_start)) {
        return 0;
    }
    return json_buffer_append_char(buffer, '"');
}

static int json_buffer_append_field_prefix(JSONBuffer *buffer,
                                           int *first_field,
                                           const char *name) {
    if (!buffer || !first_field || !name) return 0;
    if (!*first_field && !json_buffer_append_char(buffer, ',')) return 0;
    *first_field = 0;
    if (!json_buffer_append_char(buffer, '"')) return 0;
    if (!json_buffer_append_cstr(buffer, name)) return 0;
    return json_buffer_append_cstr(buffer, "\":");
}

static int json_buffer_append_string_field(JSONBuffer *buffer,
                                           int *first_field,
                                           const char *name,
                                           const char *value,
                                           size_t value_len) {
    if (!json_buffer_append_field_prefix(buffer, first_field, name)) return 0;
    return json_buffer_append_escaped(buffer, value ? value : "", value ? value_len : 0);
}

static int json_buffer_append_bool_field(JSONBuffer *buffer,
                                         int *first_field,
                                         const char *name,
                                         int value) {
    if (!json_buffer_append_field_prefix(buffer, first_field, name)) return 0;
    return json_buffer_append_cstr(buffer, value ? "true" : "false");
}

static int json_buffer_append_int_field(JSONBuffer *buffer,
                                        int *first_field,
                                        const char *name,
                                        int value) {
    if (!json_buffer_append_field_prefix(buffer, first_field, name)) return 0;
    return json_buffer_append_int(buffer, value);
}

static int json_buffer_append_hex_string(JSONBuffer *buffer,
                                         const char *data,
                                         size_t len) {
    static const char kHex[] = "0123456789abcdef";
    size_t i;

    if (!buffer || (!data && len != 0)) return 0;
    if (len > ((size_t)-1) / 2) return 0;
    if (!json_buffer_append_char(buffer, '"')) return 0;
    if (!json_buffer_reserve(buffer, len * 2)) return 0;
    for (i = 0; i < len; i++) {
        unsigned char byte = (unsigned char)data[i];
        buffer->data[buffer->len++] = kHex[(byte >> 4) & 0x0f];
        buffer->data[buffer->len++] = kHex[byte & 0x0f];
    }
    buffer->data[buffer->len] = '\0';
    return json_buffer_append_char(buffer, '"');
}

static int build_status_json(JSONBuffer *buffer,
                             const char *request_id,
                             CmdStatusCode status,
                             const char *message) {
    int first_field = 1;
    int ok;
    char error_message[TCP_ERROR_MESSAGE_MAX_BYTES + 1];
    const char *id_text;
    const char *status_text;

    if (!buffer) return 0;
    ok = status == CMD_STATUS_OK;
    id_text = request_id ? request_id : "unknown";
    status_text = cmd_status_to_string(status);
    if (!json_buffer_append_char(buffer, '{')) return 0;
    if (!json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "id",
                                         id_text,
                                         strlen(id_text))) return 0;
    if (!json_buffer_append_bool_field(buffer, &first_field, "ok", ok)) return 0;
    if (!json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "status",
                                         status_text,
                                         strlen(status_text))) return 0;
    if (!ok) {
        copy_cstr(error_message, sizeof(error_message), message ? message : "request failed");
        if (!json_buffer_append_string_field(buffer,
                                             &first_field,
                                             "error",
                                             error_message,
                                             strlen(error_message))) return 0;
    }
    if (!json_buffer_append_cstr(buffer, "}\n")) return 0;
    return 1;
}

static int write_status_response(TCPConnection *connection,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 const char *message) {
    JSONBuffer buffer;
    int rc = -1;

    json_buffer_init(&buffer);
    if (build_status_json(&buffer, request_id, status, message)) {
        rc = write_response_bytes(connection, buffer.data, buffer.len);
    }
    json_buffer_destroy(&buffer);
    return rc;
}

static int build_cmd_response_json(JSONBuffer *buffer, CmdResponse *response) {
    int first_field = 1;
    const char *status_text;

    if (!buffer || !response) return 0;
    status_text = cmd_status_to_string(response->status);
    if (!json_buffer_append_char(buffer, '{')) return 0;
    if (!json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "id",
                                         response->request_id,
                                         strlen(response->request_id))) return 0;
    if (!json_buffer_append_bool_field(buffer,
                                       &first_field,
                                       "ok",
                                       response->ok ? 1 : 0)) return 0;
    if (!json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "status",
                                         status_text,
                                         strlen(status_text))) return 0;
    if (response->body_format == CMD_BODY_TEXT &&
        !json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "body_format",
                                         "text",
                                         4)) return 0;
    if (response->body_format == CMD_BODY_JSON &&
        !json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "body_format",
                                         "json",
                                         4)) return 0;
    if (response->body_format == CMD_BODY_BINARY &&
        !json_buffer_append_string_field(buffer,
                                         &first_field,
                                         "body_format",
                                         "binary",
                                         6)) return 0;
    if (response->row_count != 0 &&
        !json_buffer_append_int_field(buffer,
                                      &first_field,
                                      "row_count",
                                      response->row_count)) return 0;
    if (response->affected_count != 0 &&
        !json_buffer_append_int_field(buffer,
                                      &first_field,
                                      "affected_count",
                                      response->affected_count)) return 0;

    if (response->body && response->body_len > 0) {
        if (response->body_format == CMD_BODY_TEXT) {
            if (!json_buffer_append_string_field(buffer,
                                                 &first_field,
                                                 "body",
                                                 response->body,
                                                 response->body_len)) return 0;
        } else if (response->body_format == CMD_BODY_JSON) {
            if (!json_buffer_append_field_prefix(buffer, &first_field, "body")) return 0;
            if (!json_buffer_append_bytes(buffer, response->body, response->body_len)) return 0;
        } else if (response->body_format == CMD_BODY_BINARY) {
            if (!json_buffer_append_field_prefix(buffer, &first_field, "body_hex")) return 0;
            if (!json_buffer_append_hex_string(buffer,
                                               response->body,
                                               response->body_len)) return 0;
        }
    }

    if (!response->ok && response->error_message) {
        if (!json_buffer_append_string_field(buffer,
                                             &first_field,
                                             "error",
                                             response->error_message,
                                             strlen(response->error_message))) return 0;
    }

    if (!json_buffer_append_cstr(buffer, "}\n")) return 0;
    return 1;
}

static void tcp_response_callback(CmdProcessor *processor,
                                  CmdRequest *request,
                                  CmdResponse *response,
                                  void *user_data) {
    TCPConnection *connection;
    const char *request_id;
    JSONBuffer buffer;

    connection = (TCPConnection *)user_data;
    request_id = request ? request->request_id : (response ? response->request_id : "unknown");

    if (response) {
        json_buffer_init(&buffer);
        if (build_cmd_response_json(&buffer, response)) {
            (void)write_response_bytes(connection, buffer.data, buffer.len);
        } else {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_INTERNAL_ERROR,
                                        "response serialization failed");
        }
        json_buffer_destroy(&buffer);
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
        while (connection->read_start < connection->read_end) {
            char ch = connection->read_buffer[connection->read_start++];

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

        {
            ssize_t n;
            int fd;

            pthread_mutex_lock(&connection->state_mutex);
            fd = connection->client_fd;
            pthread_mutex_unlock(&connection->state_mutex);
            if (fd < 0) return -1;

            n = recv(fd, connection->read_buffer, sizeof(connection->read_buffer), 0);
            if (n == 0) return -1;
            if (n < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            connection->read_start = 0;
            connection->read_end = (size_t)n;
        }
    }

    return -1;
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

static const char *skip_json_ws(const char *cursor) {
    while (cursor && *cursor && isspace((unsigned char)*cursor)) cursor++;
    return cursor;
}

static int parse_hex_digit(char ch, unsigned value[static 1]) {
    if (ch >= '0' && ch <= '9') {
        *value = (unsigned)(ch - '0');
        return 1;
    }
    if (ch >= 'a' && ch <= 'f') {
        *value = (unsigned)(10 + (ch - 'a'));
        return 1;
    }
    if (ch >= 'A' && ch <= 'F') {
        *value = (unsigned)(10 + (ch - 'A'));
        return 1;
    }
    return 0;
}

static int parse_json_string_inplace(const char **cursor_ptr,
                                     char **out_value,
                                     size_t *out_len) {
    const char *cursor;
    const char *src;
    char *dst;

    if (out_value) *out_value = NULL;
    if (out_len) *out_len = 0;
    if (!cursor_ptr || !*cursor_ptr) return 0;

    cursor = skip_json_ws(*cursor_ptr);
    if (*cursor != '"') return 0;

    src = cursor + 1;
    dst = (char *)cursor;
    while (*src) {
        if (*src == '"') {
            *dst = '\0';
            src++;
            if (out_value) *out_value = (char *)cursor;
            if (out_len) *out_len = (size_t)(dst - (char *)cursor);
            *cursor_ptr = src;
            return 1;
        }
        if (*src == '\\') {
            src++;
            if (*src == '\0') return 0;
            switch (*src) {
                case '"':
                case '\\':
                case '/':
                    *dst++ = *src++;
                    break;
                case 'b':
                    *dst++ = '\b';
                    src++;
                    break;
                case 'f':
                    *dst++ = '\f';
                    src++;
                    break;
                case 'n':
                    *dst++ = '\n';
                    src++;
                    break;
                case 'r':
                    *dst++ = '\r';
                    src++;
                    break;
                case 't':
                    *dst++ = '\t';
                    src++;
                    break;
                case 'u': {
                    unsigned codepoint = 0;
                    unsigned digit;
                    int i;
                    src++;
                    for (i = 0; i < 4; i++) {
                        if (!parse_hex_digit(src[i], &digit)) return 0;
                        codepoint = (codepoint << 4) | digit;
                    }
                    *dst++ = codepoint <= 0x7f ? (char)codepoint : '?';
                    src += 4;
                    break;
                }
                default:
                    return 0;
            }
            continue;
        }
        *dst++ = *src++;
    }
    return 0;
}

static int parse_request_line(char *line,
                              char *request_id,
                              size_t request_id_size,
                              TCPRequestOp *out_op,
                              char **out_sql) {
    const char *cursor;
    char *key;
    char *value;
    size_t key_len;
    size_t value_len;
    int saw_id = 0;
    int saw_op = 0;

    if (out_sql) *out_sql = NULL;
    if (!line || !request_id || request_id_size == 0 || !out_op) return 0;

    cursor = skip_json_ws(line);
    if (*cursor != '{') return 0;
    cursor = skip_json_ws(cursor + 1);
    if (*cursor == '}') return 0;

    while (*cursor) {
        if (!parse_json_string_inplace(&cursor, &key, &key_len)) return 0;
        cursor = skip_json_ws(cursor);
        if (*cursor != ':') return 0;
        cursor = skip_json_ws(cursor + 1);
        if (!parse_json_string_inplace(&cursor, &value, &value_len)) return 0;

        if (key_len == 2 && strcmp(key, "id") == 0) {
            if (value_len == 0) return -2;
            if (value_len > TCP_REQUEST_ID_MAX_BYTES) return -3;
            copy_cstr(request_id, request_id_size, value);
            saw_id = 1;
        } else if (key_len == 2 && strcmp(key, "op") == 0) {
            if (value_len > TCP_OP_MAX_BYTES || !parse_op(value, out_op)) return -4;
            saw_op = 1;
        } else if (key_len == 3 && strcmp(key, "sql") == 0) {
            if (out_sql) *out_sql = value;
        }

        cursor = skip_json_ws(cursor);
        if (*cursor == ',') {
            cursor = skip_json_ws(cursor + 1);
            continue;
        }
        if (*cursor == '}') {
            cursor = skip_json_ws(cursor + 1);
            if (*cursor != '\0') return 0;
            break;
        }
        return 0;
    }

    if (!saw_id) return -5;
    if (!saw_op) return -6;
    return 1;
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
                              char *line,
                              size_t line_len) {
    char request_id[TCP_REQUEST_ID_MAX_BYTES + 1];
    char *sql_value = NULL;
    TCPRequestOp op;
    int parse_rc;

    (void)line_len;
    request_id[0] = '\0';

    parse_rc = parse_request_line(line, request_id, sizeof(request_id), &op, &sql_value);
    if (parse_rc == 0) {
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "invalid JSON object");
        return;
    }
    if (parse_rc == -5) {
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "missing id");
        return;
    }
    if (parse_rc == -2) {
        (void)write_status_response(connection,
                                    "",
                                    CMD_STATUS_BAD_REQUEST,
                                    "empty id");
        return;
    }
    if (parse_rc == -3) {
        (void)write_status_response(connection,
                                    "unknown",
                                    CMD_STATUS_BAD_REQUEST,
                                    "id too long");
        return;
    }
    if (parse_rc == -6) {
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_BAD_REQUEST,
                                    "missing op");
        return;
    }
    if (parse_rc == -4) {
        (void)write_status_response(connection,
                                    request_id,
                                    CMD_STATUS_BAD_REQUEST,
                                    "unknown op");
        return;
    }
    if (parse_rc <= 0) return;

    if (op == TCP_REQUEST_OP_CLOSE) {
        (void)write_status_response(connection, request_id, CMD_STATUS_OK, NULL);
        connection_close_fd(connection);
        return;
    }

    if (op == TCP_REQUEST_OP_SQL) {
        if (!sql_value) {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_BAD_REQUEST,
                                        "missing sql");
            return;
        }
        if (strlen(sql_value) > connection->server->processor->context->max_sql_len) {
            (void)write_status_response(connection,
                                        request_id,
                                        CMD_STATUS_SQL_TOO_LONG,
                                        "sql too long");
            return;
        }
    }

    submit_parsed_request(connection, request_id, op, sql_value);
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
    init_inflight_pool(connection);
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
