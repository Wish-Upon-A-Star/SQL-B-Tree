#include "tcp_cmd_processor.h"
#include "tcp_protocol_binary.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
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

#ifndef TCP_SOCKET_BUFFER_BYTES
#define TCP_SOCKET_BUFFER_BYTES 1048576
#endif

#ifndef TCP_MAX_OUTBOUND_FRAMES
#define TCP_MAX_OUTBOUND_FRAMES 256
#endif

#ifndef TCP_MAX_OUTBOUND_BYTES
#define TCP_MAX_OUTBOUND_BYTES (4u * 1024u * 1024u)
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

typedef struct TCPOutboundFrame {
    unsigned char *data;
    size_t len;
    struct TCPOutboundFrame *next;
} TCPOutboundFrame;

typedef struct TCPConnection {
    int client_fd;
    int wake_fds[2];
    char client_key[TCP_CLIENT_KEY_BYTES];
    int closing;
    int close_after_flush;
    size_t inflight_count;
    size_t ref_count;
    size_t read_start;
    size_t read_end;
    char read_buffer[TCP_READ_BUFFER_BYTES];
    pthread_t thread;
    pthread_mutex_t state_mutex;
    pthread_mutex_t queue_mutex;
    pthread_mutex_t write_mutex;
    TCPOutboundFrame *outbound_head;
    TCPOutboundFrame *outbound_tail;
    size_t outbound_count;
    size_t outbound_bytes;
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
static void notify_connection(TCPConnection *connection);

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
    notify_connection(connection);
}

static int connection_is_closing(TCPConnection *connection) {
    int closing;

    pthread_mutex_lock(&connection->state_mutex);
    closing = connection->closing;
    pthread_mutex_unlock(&connection->state_mutex);
    return closing;
}

static void connection_request_close_after_flush(TCPConnection *connection) {
    if (!connection) return;
    pthread_mutex_lock(&connection->state_mutex);
    connection->close_after_flush = 1;
    pthread_mutex_unlock(&connection->state_mutex);
    notify_connection(connection);
}

static int connection_should_close_after_flush(TCPConnection *connection) {
    int should_close;

    if (!connection) return 0;
    pthread_mutex_lock(&connection->state_mutex);
    should_close = connection->close_after_flush;
    pthread_mutex_unlock(&connection->state_mutex);
    return should_close;
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

static void free_outbound_frames(TCPConnection *connection) {
    TCPOutboundFrame *frame;
    TCPOutboundFrame *next;

    if (!connection) return;
    frame = connection->outbound_head;
    while (frame) {
        next = frame->next;
        free(frame->data);
        free(frame);
        frame = next;
    }
    connection->outbound_head = NULL;
    connection->outbound_tail = NULL;
    connection->outbound_count = 0;
    connection->outbound_bytes = 0;
}

static void connection_destroy(TCPConnection *connection) {
    if (!connection) return;

    connection_close_fd(connection);
    remove_connection_from_server(connection);
    release_connection_slot(connection->server, connection->client_key);
    if (connection->wake_fds[0] >= 0) close(connection->wake_fds[0]);
    if (connection->wake_fds[1] >= 0) close(connection->wake_fds[1]);

    free_inflight_list(connection);
    free_outbound_frames(connection);
    pthread_mutex_destroy(&connection->state_mutex);
    pthread_mutex_destroy(&connection->queue_mutex);
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

static void notify_connection(TCPConnection *connection) {
    unsigned char wake = 1;
    int wake_fd = -1;

    if (!connection) return;
    pthread_mutex_lock(&connection->state_mutex);
    wake_fd = connection->wake_fds[1];
    pthread_mutex_unlock(&connection->state_mutex);
    if (wake_fd >= 0) {
        (void)write(wake_fd, &wake, 1);
    }
}

static int enqueue_response_frame(TCPConnection *connection,
                                  unsigned char *frame_bytes,
                                  size_t frame_len) {
    TCPOutboundFrame *frame;
    int ok = 0;

    if (!connection || !frame_bytes || frame_len == 0) return 0;
    frame = (TCPOutboundFrame *)calloc(1, sizeof(*frame));
    if (!frame) return 0;
    frame->data = frame_bytes;
    frame->len = frame_len;

    pthread_mutex_lock(&connection->queue_mutex);
    if (!connection->closing &&
        connection->outbound_count < TCP_MAX_OUTBOUND_FRAMES &&
        connection->outbound_bytes + frame_len <= TCP_MAX_OUTBOUND_BYTES) {
        if (connection->outbound_tail) {
            connection->outbound_tail->next = frame;
        } else {
            connection->outbound_head = frame;
        }
        connection->outbound_tail = frame;
        connection->outbound_count++;
        connection->outbound_bytes += frame_len;
        ok = 1;
    }
    pthread_mutex_unlock(&connection->queue_mutex);

    if (!ok) {
        free(frame->data);
        free(frame);
        return 0;
    }
    notify_connection(connection);
    return 1;
}

static TCPOutboundFrame *pop_outbound_frame(TCPConnection *connection) {
    TCPOutboundFrame *frame;

    if (!connection) return NULL;
    pthread_mutex_lock(&connection->queue_mutex);
    frame = connection->outbound_head;
    if (frame) {
        connection->outbound_head = frame->next;
        if (!connection->outbound_head) connection->outbound_tail = NULL;
        connection->outbound_count--;
        connection->outbound_bytes -= frame->len;
        frame->next = NULL;
    }
    pthread_mutex_unlock(&connection->queue_mutex);
    return frame;
}

static int flush_outbound_queue(TCPConnection *connection) {
    TCPOutboundFrame *frame;
    int fd = -1;

    if (!connection) return -1;
    while ((frame = pop_outbound_frame(connection)) != NULL) {
        pthread_mutex_lock(&connection->write_mutex);
        pthread_mutex_lock(&connection->state_mutex);
        if (!connection->closing && connection->client_fd >= 0) fd = connection->client_fd;
        else fd = -1;
        pthread_mutex_unlock(&connection->state_mutex);

        if (fd < 0 || send_all(fd, (const char *)frame->data, frame->len) != 0) {
            pthread_mutex_unlock(&connection->write_mutex);
            free(frame->data);
            free(frame);
            return -1;
        }
        pthread_mutex_unlock(&connection->write_mutex);
        free(frame->data);
        free(frame);
    }
    return 0;
}

static void drain_connection_wakeup(TCPConnection *connection) {
    unsigned char buffer[64];
    int wake_fd = -1;

    if (!connection) return;
    pthread_mutex_lock(&connection->state_mutex);
    if (!connection->closing) wake_fd = connection->wake_fds[0];
    pthread_mutex_unlock(&connection->state_mutex);
    if (wake_fd < 0) return;

    while (1) {
        ssize_t n = read(wake_fd, buffer, sizeof(buffer));
        if (n > 0) continue;
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        break;
    }
}

static int build_response_frame(const char *request_id,
                                CmdStatusCode status,
                                int ok,
                                CmdBodyFormat body_format,
                                const char *body,
                                size_t body_len,
                                const char *error_message,
                                int row_count,
                                int affected_count,
                                unsigned char **out_frame,
                                size_t *out_len) {
    TCPBinaryResponseHeader header;
    unsigned char *frame;
    size_t header_size;
    size_t request_id_len;
    size_t error_len = 0;
    size_t frame_len;
    unsigned char *cursor;

    if (out_frame) *out_frame = NULL;
    if (out_len) *out_len = 0;
    if (!out_frame || !out_len) return 0;

    request_id_len = request_id ? strlen(request_id) : 0;
    if (request_id_len > TCP_REQUEST_ID_MAX_BYTES) return 0;
    if (error_message) error_len = strlen(error_message);

    tcp_binary_init_response_header(&header,
                                    status,
                                    ok,
                                    body_format,
                                    request_id_len,
                                    body_len,
                                    error_len,
                                    row_count,
                                    affected_count);
    frame_len = tcp_binary_response_frame_size(&header);
    header_size = tcp_binary_response_header_size();
    frame = (unsigned char *)calloc(frame_len, 1);
    if (!frame) return 0;

    tcp_binary_encode_response_header(frame, &header);
    cursor = frame + header_size;
    if (request_id_len > 0) {
        memcpy(cursor, request_id, request_id_len);
        cursor += request_id_len;
    }
    if (body_len > 0) {
        memcpy(cursor, body, body_len);
        cursor += body_len;
    }
    if (error_len > 0) {
        memcpy(cursor, error_message, error_len);
    }

    *out_frame = frame;
    *out_len = frame_len;
    return 1;
}

static int write_status_response(TCPConnection *connection,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 const char *message) {
    unsigned char *frame = NULL;
    size_t frame_len = 0;
    if (!build_response_frame(request_id ? request_id : "unknown",
                              status,
                              status == CMD_STATUS_OK,
                              CMD_BODY_NONE,
                              NULL,
                              0,
                              status == CMD_STATUS_OK ? NULL : (message ? message : "request failed"),
                              0,
                              0,
                              &frame,
                              &frame_len)) {
        return -1;
    }
    if (!enqueue_response_frame(connection, frame, frame_len)) {
        free(frame);
        return -1;
    }
    return 0;
}

static void send_status_or_close(TCPConnection *connection,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 const char *message) {
    if (write_status_response(connection, request_id, status, message) != 0) {
        connection_close_fd(connection);
    }
}

static int write_cmd_response(TCPConnection *connection, CmdResponse *response) {
    unsigned char *frame = NULL;
    size_t frame_len = 0;

    if (!response) return -1;
    if (!build_response_frame(response->request_id,
                              response->status,
                              response->ok ? 1 : 0,
                              response->body_format,
                              response->body,
                              response->body_len,
                              response->error_message,
                              response->row_count,
                              response->affected_count,
                              &frame,
                              &frame_len)) {
        return -1;
    }
    if (!enqueue_response_frame(connection, frame, frame_len)) {
        free(frame);
        return -1;
    }
    return 0;
}

static void tcp_response_callback(CmdProcessor *processor,
                                  CmdRequest *request,
                                  CmdResponse *response,
                                  void *user_data) {
    TCPConnection *connection;
    const char *request_id;

    connection = (TCPConnection *)user_data;
    request_id = request ? request->request_id : (response ? response->request_id : "unknown");

    if (response) {
        if (write_cmd_response(connection, response) != 0) {
            connection_close_fd(connection);
        }
    } else {
        if (write_status_response(connection,
                                  request_id,
                                  CMD_STATUS_INTERNAL_ERROR,
                                  "processor returned no response") != 0) {
            connection_close_fd(connection);
        }
    }

    remove_inflight(connection, request_id);
    if (response) cmd_processor_release_response(processor, response);
    if (request) cmd_processor_release_request(processor, request);
    connection_release(connection);
}

static int connection_read_more(TCPConnection *connection) {
    ssize_t n;
    int fd;

    pthread_mutex_lock(&connection->state_mutex);
    fd = connection->client_fd;
    pthread_mutex_unlock(&connection->state_mutex);
    if (fd < 0) return -1;

    n = recv(fd, connection->read_buffer, sizeof(connection->read_buffer), 0);
    if (n == 0) return -1;
    if (n < 0) {
        if (errno == EINTR) return 1;
        return -1;
    }
    connection->read_start = 0;
    connection->read_end = (size_t)n;
    return 0;
}

static int read_exact_bytes(TCPConnection *connection,
                            unsigned char *dst,
                            size_t len) {
    size_t copied = 0;

    if (!connection || (!dst && len != 0)) return -1;
    while (copied < len && !connection_is_closing(connection)) {
        size_t available = connection->read_end - connection->read_start;
        size_t chunk = len - copied;

        if (available == 0) {
            int rc = connection_read_more(connection);
            if (rc > 0) continue;
            if (rc < 0) return -1;
            available = connection->read_end - connection->read_start;
            if (available == 0) continue;
        }
        if (chunk > available) chunk = available;
        memcpy(dst + copied, connection->read_buffer + connection->read_start, chunk);
        connection->read_start += chunk;
        copied += chunk;
    }
    return copied == len ? 0 : -1;
}

static int read_request_frame(TCPConnection *connection,
                              unsigned char **out_frame,
                              size_t *out_frame_len,
                              TCPBinaryDecodedRequest *out_decoded) {
    TCPBinaryRequestHeader header;
    unsigned char *frame = NULL;
    size_t header_size;
    size_t frame_len;
    int ok = 0;

    if (out_frame) *out_frame = NULL;
    if (out_frame_len) *out_frame_len = 0;
    if (!out_frame || !out_frame_len || !out_decoded) return -1;

    header_size = tcp_binary_request_header_size();
    frame = (unsigned char *)calloc(header_size, 1);
    if (!frame) return -1;
    if (read_exact_bytes(connection, frame, header_size) != 0) goto cleanup;
    if (!tcp_binary_decode_request_header(frame, header_size, &header)) goto cleanup;
    if (!tcp_binary_validate_request_header(&header)) goto cleanup;

    frame_len = tcp_binary_request_frame_size(&header);
    if (frame_len > TCP_MAX_FRAME_BYTES || frame_len < header_size) goto cleanup;
    if (frame_len > header_size) {
        unsigned char *grown = (unsigned char *)realloc(frame, frame_len);
        if (!grown) goto cleanup;
        frame = grown;
        if (read_exact_bytes(connection, frame + header_size, frame_len - header_size) != 0) goto cleanup;
    }
    if (!tcp_binary_decode_request_frame(frame, frame_len, out_decoded)) goto cleanup;
    *out_frame = frame;
    *out_frame_len = frame_len;
    ok = 1;

cleanup:
    if (!ok) free(frame);
    return ok ? 0 : -1;
}

static int decode_request_op(TCPBinaryOp op, TCPRequestOp *out_op) {
    if (!out_op) return 0;
    switch (op) {
        case TCP_BINARY_OP_SQL:
            *out_op = TCP_REQUEST_OP_SQL;
            return 1;
        case TCP_BINARY_OP_PING:
            *out_op = TCP_REQUEST_OP_PING;
            return 1;
        case TCP_BINARY_OP_CLOSE:
            *out_op = TCP_REQUEST_OP_CLOSE;
            return 1;
        default:
            return 0;
    }
}

static void submit_parsed_request(TCPConnection *connection,
                                  const char *request_id,
                                  TCPRequestOp op,
                                  const char *sql,
                                  size_t sql_len) {
    CmdProcessor *processor;
    CmdRequest *cmd_request = NULL;
    CmdStatusCode status;
    const char *message;
    int submit_rc;
    char *sql_copy = NULL;

    processor = connection->server->processor;
    if (!register_inflight(connection, request_id, &status, &message)) {
        send_status_or_close(connection, request_id, status, message);
        return;
    }

    if (cmd_processor_acquire_request(processor, &cmd_request) != 0) {
        remove_inflight(connection, request_id);
        send_status_or_close(connection,
                             request_id,
                             CMD_STATUS_INTERNAL_ERROR,
                             "request slot unavailable");
        return;
    }

    if (op == TCP_REQUEST_OP_SQL) {
        sql_copy = (char *)calloc(sql_len + 1, 1);
        if (!sql_copy) {
            cmd_processor_release_request(processor, cmd_request);
            remove_inflight(connection, request_id);
            send_status_or_close(connection,
                                 request_id,
                                 CMD_STATUS_INTERNAL_ERROR,
                                 "request allocation failed");
            return;
        }
        if (sql_len > 0) memcpy(sql_copy, sql, sql_len);
        status = cmd_processor_set_sql_request(processor, cmd_request, request_id, sql_copy);
    } else {
        status = cmd_processor_set_ping_request(processor, cmd_request, request_id);
    }
    free(sql_copy);

    if (status != CMD_STATUS_OK) {
        cmd_processor_release_request(processor, cmd_request);
        remove_inflight(connection, request_id);
        send_status_or_close(connection, request_id, status, "invalid request");
        return;
    }

    if (!connection_add_ref(connection)) {
        cmd_processor_release_request(processor, cmd_request);
        remove_inflight(connection, request_id);
        send_status_or_close(connection,
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
        send_status_or_close(connection,
                             request_id,
                             CMD_STATUS_INTERNAL_ERROR,
                             "submit failed");
    }
}

static void process_request_frame(TCPConnection *connection,
                                  const TCPBinaryDecodedRequest *decoded) {
    char request_id[TCP_REQUEST_ID_MAX_BYTES + 1];
    TCPRequestOp op;
    const char *sql_value = NULL;
    size_t sql_len = 0;

    if (!connection || !decoded) return;
    request_id[0] = '\0';

    if (decoded->header.request_id_len == 0) {
        send_status_or_close(connection,
                             "unknown",
                             CMD_STATUS_BAD_REQUEST,
                             "empty id");
        return;
    }
    if (decoded->header.request_id_len > TCP_REQUEST_ID_MAX_BYTES) {
        send_status_or_close(connection,
                             "unknown",
                             CMD_STATUS_BAD_REQUEST,
                             "id too long");
        return;
    }
    memcpy(request_id, decoded->request_id, decoded->header.request_id_len);
    request_id[decoded->header.request_id_len] = '\0';

    if (!decode_request_op((TCPBinaryOp)decoded->header.op, &op)) {
        send_status_or_close(connection,
                             request_id,
                             CMD_STATUS_BAD_REQUEST,
                             "unknown op");
        return;
    }

    if (op == TCP_REQUEST_OP_CLOSE) {
        if (write_status_response(connection, request_id, CMD_STATUS_OK, NULL) != 0) {
            connection_close_fd(connection);
            return;
        }
        connection_request_close_after_flush(connection);
        return;
    }

    if (op == TCP_REQUEST_OP_SQL) {
        sql_len = decoded->header.payload_len;
        if (sql_len == 0 || !decoded->payload) {
            send_status_or_close(connection,
                                 request_id,
                                 CMD_STATUS_BAD_REQUEST,
                                 "missing sql");
            return;
        }
        if (sql_len > connection->server->processor->context->max_sql_len) {
            send_status_or_close(connection,
                                 request_id,
                                 CMD_STATUS_SQL_TOO_LONG,
                                 "sql too long");
            return;
        }
        sql_value = (const char *)decoded->payload;
    }

    submit_parsed_request(connection, request_id, op, sql_value, sql_len);
}

static void *connection_thread_main(void *arg) {
    TCPConnection *connection;
    unsigned char *frame = NULL;
    size_t frame_len = 0;
    TCPBinaryDecodedRequest decoded;
    struct pollfd fds[2];
    int fd_count;

    connection = (TCPConnection *)arg;

    while (!connection_is_closing(connection)) {
        int client_fd;
        int wake_fd;

        if (flush_outbound_queue(connection) != 0) break;
        if (connection_should_close_after_flush(connection)) break;

        pthread_mutex_lock(&connection->state_mutex);
        client_fd = connection->client_fd;
        wake_fd = connection->wake_fds[0];
        pthread_mutex_unlock(&connection->state_mutex);
        if (client_fd < 0) break;

        fd_count = 0;
        fds[fd_count].fd = client_fd;
        fds[fd_count].events = POLLIN;
        fds[fd_count].revents = 0;
        fd_count++;
        if (wake_fd >= 0) {
            fds[fd_count].fd = wake_fd;
            fds[fd_count].events = POLLIN;
            fds[fd_count].revents = 0;
            fd_count++;
        }

        if (poll(fds, (nfds_t)fd_count, -1) < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (fd_count > 1 && (fds[1].revents & (POLLIN | POLLHUP))) {
            drain_connection_wakeup(connection);
            continue;
        }
        if ((fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) break;
        if ((fds[0].revents & POLLIN) == 0) continue;

        if (read_request_frame(connection, &frame, &frame_len, &decoded) != 0) {
            send_status_or_close(connection,
                                 "unknown",
                                 CMD_STATUS_BAD_REQUEST,
                                 "invalid frame");
            if (flush_outbound_queue(connection) != 0) {
                break;
            }
            continue;
        }
        process_request_frame(connection, &decoded);
        free(frame);
        frame = NULL;
        frame_len = 0;
    }

    (void)flush_outbound_queue(connection);
    free(frame);
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
    int wake_fds[2] = { -1, -1 };

    connection = (TCPConnection *)calloc(1, sizeof(*connection));
    if (!connection) return NULL;

    connection->wake_fds[0] = -1;
    connection->wake_fds[1] = -1;
    connection->client_fd = client_fd;
    connection->server = server;
    connection->ref_count = 1;
    copy_cstr(connection->client_key, sizeof(connection->client_key), client_key);
    init_inflight_pool(connection);
    if (pthread_mutex_init(&connection->state_mutex, NULL) != 0) {
        free(connection);
        return NULL;
    }
    if (pthread_mutex_init(&connection->queue_mutex, NULL) != 0) {
        pthread_mutex_destroy(&connection->state_mutex);
        free(connection);
        return NULL;
    }
    if (pthread_mutex_init(&connection->write_mutex, NULL) != 0) {
        pthread_mutex_destroy(&connection->queue_mutex);
        pthread_mutex_destroy(&connection->state_mutex);
        free(connection);
        return NULL;
    }
    if (pipe(wake_fds) != 0) {
        pthread_mutex_destroy(&connection->write_mutex);
        pthread_mutex_destroy(&connection->queue_mutex);
        pthread_mutex_destroy(&connection->state_mutex);
        free(connection);
        return NULL;
    }
    if (fcntl(wake_fds[0], F_SETFL, O_NONBLOCK) != 0 ||
        fcntl(wake_fds[1], F_SETFL, O_NONBLOCK) != 0) {
        close(wake_fds[0]);
        close(wake_fds[1]);
        pthread_mutex_destroy(&connection->write_mutex);
        pthread_mutex_destroy(&connection->queue_mutex);
        pthread_mutex_destroy(&connection->state_mutex);
        free(connection);
        return NULL;
    }
    connection->wake_fds[0] = wake_fds[0];
    connection->wake_fds[1] = wake_fds[1];

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
        TCP_MAX_FRAME_BYTES <= 0 ||
        TCP_REQUEST_ID_MAX_BYTES <= 0 ||
        TCP_MAX_OUTBOUND_FRAMES <= 0 ||
        TCP_MAX_OUTBOUND_BYTES == 0) {
        return 0;
    }
    if (TCP_REQUEST_ID_MAX_BYTES >= (int)sizeof(((CmdRequest *)0)->request_id)) return 0;
    if (TCP_MAX_CONNECTIONS_PER_CLIENT > TCP_MAX_CONNECTIONS_TOTAL) return 0;
    if (TCP_MAX_INFLIGHT_PER_CONNECTION > TCP_MAX_INFLIGHT_PER_CLIENT) return 0;
    if (TCP_MAX_FRAME_BYTES < tcp_binary_request_header_size()) return 0;
    if (TCP_MAX_FRAME_BYTES < config->processor->context->max_sql_len) return 0;
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
