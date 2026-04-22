#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cmd_processor/cmd_processor.h"
#include "cmd_processor/engine_cmd_processor.h"
#include "cmd_processor/tcp_cmd_processor.h"

typedef struct {
    const char *host;
    int port;
    int worker_count;
    int shard_count;
    int queue_capacity_per_shard;
    int planner_cache_capacity;
} TCPServerConfig;

static volatile sig_atomic_t g_stop_requested = 0;

static void request_shutdown(int signum) {
    (void)signum;
    g_stop_requested = 1;
}

static void init_tcp_server_config(TCPServerConfig *config) {
    if (!config) return;
    memset(config, 0, sizeof(*config));
    config->host = "0.0.0.0";
    config->port = 15432;
    config->worker_count = 4;
    config->shard_count = 4;
    config->queue_capacity_per_shard = 128;
    config->planner_cache_capacity = 256;
}

static int consume_arg_value(int argc,
                             char *argv[],
                             int *index,
                             const char **out_value) {
    if (!index || !out_value) return 0;
    if (*index + 1 >= argc) return 0;
    *index += 1;
    *out_value = argv[*index];
    return 1;
}

static int parse_int_value(const char *value, int minimum) {
    long parsed;
    char *end = NULL;

    if (!value || value[0] == '\0') return -1;
    parsed = strtol(value, &end, 10);
    if (!end || *end != '\0') return -1;
    if (parsed < minimum || parsed > 65535) return -1;
    return (int)parsed;
}

static void print_usage(const char *program_name) {
    fprintf(stderr,
            "usage: %s [--host <addr>] [--port <port>] [--workers <n>] "
            "[--shards <n>] [--queue-capacity <n>] [--planner-cache <n>]\n",
            program_name ? program_name : "tcp_sql_server");
}

static int parse_tcp_server_config(int argc, char *argv[], TCPServerConfig *config) {
    int i;

    if (!config) return 0;
    init_tcp_server_config(config);

    for (i = 1; i < argc; i++) {
        const char *value = NULL;
        int parsed = -1;

        if (strcmp(argv[i], "--host") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value) || !value || value[0] == '\0') {
                return 0;
            }
            config->host = value;
            continue;
        }
        if (strcmp(argv[i], "--port") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value)) return 0;
            parsed = parse_int_value(value, 1);
            if (parsed < 0) return 0;
            config->port = parsed;
            continue;
        }
        if (strcmp(argv[i], "--workers") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value)) return 0;
            parsed = parse_int_value(value, 1);
            if (parsed < 0) return 0;
            config->worker_count = parsed;
            continue;
        }
        if (strcmp(argv[i], "--shards") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value)) return 0;
            parsed = parse_int_value(value, 1);
            if (parsed < 0) return 0;
            config->shard_count = parsed;
            continue;
        }
        if (strcmp(argv[i], "--queue-capacity") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value)) return 0;
            parsed = parse_int_value(value, 1);
            if (parsed < 0) return 0;
            config->queue_capacity_per_shard = parsed;
            continue;
        }
        if (strcmp(argv[i], "--planner-cache") == 0) {
            if (!consume_arg_value(argc, argv, &i, &value)) return 0;
            parsed = parse_int_value(value, 1);
            if (parsed < 0) return 0;
            config->planner_cache_capacity = parsed;
            continue;
        }
        return 0;
    }

    return 1;
}

static int install_signal_handlers(void) {
    if (signal(SIGINT, request_shutdown) == SIG_ERR) return 0;
    if (signal(SIGTERM, request_shutdown) == SIG_ERR) return 0;
    return 1;
}

int main(int argc, char *argv[]) {
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;
    TCPCmdProcessorConfig tcp_config;
    TCPServerConfig config;
    CmdProcessor *processor = NULL;
    TCPCmdProcessor *server = NULL;
    int rc = 1;

    if (!parse_tcp_server_config(argc, argv, &config)) {
        print_usage(argc > 0 ? argv[0] : "tcp_sql_server");
        return 1;
    }
    if (!install_signal_handlers()) {
        fprintf(stderr, "[tcp] failed to install signal handlers\n");
        return 1;
    }

    memset(&context, 0, sizeof(context));
    context.name = "sqlsprocessor_tcp_server";
    context.max_sql_len = 4095;
    context.request_buffer_count = 0;
    context.response_body_capacity = 4096;

    memset(&options, 0, sizeof(options));
    options.worker_count = config.worker_count;
    options.shard_count = config.shard_count;
    options.queue_capacity_per_shard = config.queue_capacity_per_shard;
    options.planner_cache_capacity = config.planner_cache_capacity;

    if (engine_cmd_processor_create(&context, &options, &processor) != 0) {
        fprintf(stderr, "[tcp] failed to create engine cmd processor\n");
        return 1;
    }

    tcp_cmd_processor_config_init(&tcp_config, processor);
    tcp_config.host = config.host;
    tcp_config.port = config.port;
    tcp_config.backlog = 64;
    if (tcp_cmd_processor_start(&tcp_config, &server) != 0) {
        fprintf(stderr, "[tcp] failed to start server on %s:%d\n", config.host, config.port);
        cmd_processor_shutdown(processor);
        return 1;
    }

    printf("[tcp] listening on %s:%d workers=%d shards=%d queue=%d planner_cache=%d\n",
           config.host,
           tcp_cmd_processor_get_port(server),
           config.worker_count,
           config.shard_count,
           config.queue_capacity_per_shard,
           config.planner_cache_capacity);
    fflush(stdout);

    while (!g_stop_requested) {
        sleep(1);
    }

    tcp_cmd_processor_stop(server);
    cmd_processor_shutdown(processor);
    rc = 0;
    return rc;
}
