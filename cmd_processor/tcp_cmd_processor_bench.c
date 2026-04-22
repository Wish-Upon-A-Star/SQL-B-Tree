#include "tcp_cmd_processor.h"
#include "engine_cmd_processor.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

typedef struct {
    int fd;
    char data[8192];
    size_t start;
    size_t end;
} BufferedReader;

static uint64_t monotonic_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static void set_client_timeout(int fd) {
    struct timeval timeout;
    int tcp_nodelay = 1;

    timeout.tv_sec = 30;
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay));
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

static int send_request_line(int fd,
                             unsigned long long id_value,
                             const char *op,
                             const char *sql) {
    char line[1024];
    int line_len;

    if (sql) {
        line_len = snprintf(line,
                            sizeof(line),
                            "{\"id\":\"req-%llu\",\"op\":\"%s\",\"sql\":\"%s\"}\n",
                            id_value,
                            op,
                            sql);
    } else {
        line_len = snprintf(line,
                            sizeof(line),
                            "{\"id\":\"req-%llu\",\"op\":\"%s\"}\n",
                            id_value,
                            op);
    }
    if (line_len <= 0 || (size_t)line_len >= sizeof(line)) return -1;
    return send_all_client(fd, line, (size_t)line_len);
}

static void reader_init(BufferedReader *reader, int fd) {
    reader->fd = fd;
    reader->start = 0;
    reader->end = 0;
}

static int reader_fill(BufferedReader *reader) {
    ssize_t n;

    if (reader->start < reader->end) return 1;
    n = recv(reader->fd, reader->data, sizeof(reader->data), 0);
    if (n <= 0) return 0;
    reader->start = 0;
    reader->end = (size_t)n;
    return 1;
}

static int read_json_line(BufferedReader *reader, char *line, size_t line_size) {
    size_t len = 0;

    if (!line || line_size == 0) return -1;
    while (len + 1 < line_size) {
        size_t i;

        if (!reader_fill(reader)) return -1;
        for (i = reader->start; i < reader->end; i++) {
            char ch = reader->data[i];
            if (ch == '\n') {
                reader->start = i + 1;
                if (len > 0 && line[len - 1] == '\r') len--;
                line[len] = '\0';
                return 0;
            }
            line[len++] = ch;
        }
        reader->start = reader->end;
    }
    return -1;
}

static int expect_ok_line(BufferedReader *reader) {
    char line[8192];

    if (read_json_line(reader, line, sizeof(line)) != 0) {
        fprintf(stderr, "[fail] no complete JSON line received\n");
        return -1;
    }
    if (!strstr(line, "\"status\":\"OK\"")) {
        fprintf(stderr, "[fail] unexpected response: %s\n", line);
        return -1;
    }
    if (!strstr(line, "\"ok\":true")) {
        fprintf(stderr, "[fail] unexpected response: %s\n", line);
        return -1;
    }
    return 0;
}

static int start_real_server(CmdProcessor **out_processor,
                             TCPCmdProcessor **out_server,
                             int *out_port) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    TCPCmdProcessorConfig tcp_config;
    int port;

    memset(&context, 0, sizeof(context));
    memset(&options, 0, sizeof(options));

    context.name = "tcp_real_bench";
    context.max_sql_len = 4095;
    context.request_buffer_count = 0;
    context.response_body_capacity = 4096;

    options.worker_count = 2;
    options.shard_count = 1;
    options.queue_capacity_per_shard = 64;
    options.planner_cache_capacity = 128;

    if (engine_cmd_processor_create(&context, &options, &processor) != 0) return -1;

    tcp_cmd_processor_config_init(&tcp_config, processor);
    if (tcp_cmd_processor_start(&tcp_config, &server) != 0) {
        cmd_processor_shutdown(processor);
        return -1;
    }

    port = tcp_cmd_processor_get_port(server);
    if (port <= 0) {
        tcp_cmd_processor_stop(server);
        cmd_processor_shutdown(processor);
        return -1;
    }

    *out_processor = processor;
    *out_server = server;
    *out_port = port;
    return 0;
}

static int run_sequential_case(int port,
                               const char *case_name,
                               const char *op,
                               const char *sql,
                               int request_count) {
    BufferedReader reader;
    uint64_t start_us;
    uint64_t end_us;
    int fd;
    int i;

    fd = connect_client(port);
    if (fd < 0) {
        fprintf(stderr, "[fail] %s: connect failed\n", case_name);
        return -1;
    }

    reader_init(&reader, fd);
    start_us = monotonic_us();
    for (i = 0; i < request_count; i++) {
        if (send_request_line(fd, (unsigned long long)i, op, sql) != 0) {
            fprintf(stderr, "[fail] %s: send failed at %d\n", case_name, i);
            close(fd);
            return -1;
        }
        if (expect_ok_line(&reader) != 0) {
            fprintf(stderr, "[fail] %s: bad response at %d\n", case_name, i);
            close(fd);
            return -1;
        }
    }
    end_us = monotonic_us();
    close(fd);

    printf("[bench] case=%s requests=%d elapsed_ms=%.2f rps=%.2f\n",
           case_name,
           request_count,
           (double)(end_us - start_us) / 1000.0,
           (double)request_count * 1000000.0 / (double)(end_us - start_us));
    return 0;
}

static int run_pipelined_case(int port,
                              const char *case_name,
                              const char *op,
                              const char *sql,
                              int request_count,
                              int window_size) {
    BufferedReader reader;
    uint64_t start_us;
    uint64_t end_us;
    int fd;
    int sent = 0;
    int received = 0;

    fd = connect_client(port);
    if (fd < 0) {
        fprintf(stderr, "[fail] %s: connect failed\n", case_name);
        return -1;
    }

    reader_init(&reader, fd);
    start_us = monotonic_us();
    while (received < request_count) {
        while (sent < request_count && sent - received < window_size) {
            if (send_request_line(fd, (unsigned long long)sent, op, sql) != 0) {
                fprintf(stderr, "[fail] %s: send failed at %d\n", case_name, sent);
                close(fd);
                return -1;
            }
            sent++;
        }
        if (expect_ok_line(&reader) != 0) {
            fprintf(stderr, "[fail] %s: bad response at %d\n", case_name, received);
            close(fd);
            return -1;
        }
        received++;
    }
    end_us = monotonic_us();
    close(fd);

    printf("[bench] case=%s requests=%d window=%d elapsed_ms=%.2f rps=%.2f\n",
           case_name,
           request_count,
           window_size,
           (double)(end_us - start_us) / 1000.0,
           (double)request_count * 1000000.0 / (double)(end_us - start_us));
    return 0;
}

int main(int argc, char **argv) {
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int port = -1;
    int request_count = 10000;
    const char *sql = "SELECT * FROM case_basic_users WHERE id = 2;";
    const char *mode = "all";
    int rc = 1;

    if (argc >= 2) request_count = atoi(argv[1]);
    if (argc >= 3) mode = argv[2];
    if (request_count <= 0) request_count = 10000;

    if (start_real_server(&processor, &server, &port) != 0) {
        fprintf(stderr, "[fail] unable to start real tcp server\n");
        return 1;
    }

    printf("[bench] port=%d requests=%d mode=%s\n", port, request_count, mode);

    if (strcmp(mode, "ping-only") == 0) {
        if (run_sequential_case(port, "ping-sequential", "ping", NULL, request_count) != 0) goto cleanup;
        if (run_pipelined_case(port, "ping-pipelined", "ping", NULL, request_count, 16) != 0) goto cleanup;
    } else {
        if (run_sequential_case(port, "ping-sequential", "ping", NULL, request_count) != 0) goto cleanup;
        if (run_pipelined_case(port, "ping-pipelined", "ping", NULL, request_count, 16) != 0) goto cleanup;
        if (run_sequential_case(port, "sql-sequential", "sql", sql, request_count) != 0) goto cleanup;
        if (run_pipelined_case(port, "sql-pipelined", "sql", sql, request_count, 16) != 0) goto cleanup;
    }

    rc = 0;

cleanup:
    if (server) tcp_cmd_processor_stop(server);
    if (processor) cmd_processor_shutdown(processor);
    return rc;
}
