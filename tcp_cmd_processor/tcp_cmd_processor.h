#ifndef TCP_CMD_PROCESSOR_H
#define TCP_CMD_PROCESSOR_H

#include "../cmd_processor/cmd_processor.h"

#ifndef TCP_DEFAULT_HOST
#define TCP_DEFAULT_HOST "127.0.0.1"
#endif

#ifndef TCP_DEFAULT_PORT
#define TCP_DEFAULT_PORT 0
#endif

#ifndef TCP_DEFAULT_BACKLOG
#define TCP_DEFAULT_BACKLOG 16
#endif

#ifndef TCP_MAX_CONNECTIONS_TOTAL
#define TCP_MAX_CONNECTIONS_TOTAL 128
#endif

#ifndef TCP_MAX_CONNECTIONS_PER_CLIENT
#define TCP_MAX_CONNECTIONS_PER_CLIENT 4
#endif

#ifndef TCP_MAX_INFLIGHT_PER_CONNECTION
#define TCP_MAX_INFLIGHT_PER_CONNECTION 16
#endif

#ifndef TCP_MAX_INFLIGHT_PER_CLIENT
#define TCP_MAX_INFLIGHT_PER_CLIENT 64
#endif

#ifndef TCP_MAX_LINE_BYTES
#define TCP_MAX_LINE_BYTES 8192
#endif

#ifndef TCP_READ_TIMEOUT_MS
#define TCP_READ_TIMEOUT_MS 30000
#endif

#ifndef TCP_WRITE_TIMEOUT_MS
#define TCP_WRITE_TIMEOUT_MS 30000
#endif

#ifndef TCP_REQUEST_ID_MAX_BYTES
#define TCP_REQUEST_ID_MAX_BYTES 63
#endif

#ifndef TCP_OP_MAX_BYTES
#define TCP_OP_MAX_BYTES 15
#endif

#ifndef TCP_ERROR_MESSAGE_MAX_BYTES
#define TCP_ERROR_MESSAGE_MAX_BYTES 256
#endif

typedef struct TCPCmdProcessor TCPCmdProcessor;

typedef struct {
    const char *host;
    int port;
    int backlog;
    CmdProcessor *processor;
} TCPCmdProcessorConfig;

void tcp_cmd_processor_config_init(TCPCmdProcessorConfig *config,
                                   CmdProcessor *processor);

int tcp_cmd_processor_start(const TCPCmdProcessorConfig *config,
                            TCPCmdProcessor **out_server);

int tcp_cmd_processor_get_port(TCPCmdProcessor *server);

void tcp_cmd_processor_stop(TCPCmdProcessor *server);

#endif
