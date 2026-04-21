#ifndef CMD_PROCESSOR_H
#define CMD_PROCESSOR_H

#include <stddef.h>

/*
 * Public request/response contract for frontends such as CLI or TCP.
 *
 * Frontends own the control flow, but request/response objects and their
 * backing buffers are owned by the CmdProcessor implementation. Frontends
 * acquire a request slot, populate it, submit it, and then release both the
 * request and the response from the callback.
 */

typedef enum {
    CMD_REQUEST_SQL,
    CMD_REQUEST_PING
} CmdRequestType;

typedef enum {
    CMD_STATUS_OK,
    CMD_STATUS_BAD_REQUEST,
    CMD_STATUS_SQL_TOO_LONG,
    CMD_STATUS_PARSE_ERROR,
    CMD_STATUS_PROCESSING_ERROR,
    CMD_STATUS_BUSY,
    /* Reserved for future implementations. v1 does not emit TIMEOUT. */
    CMD_STATUS_TIMEOUT,
    CMD_STATUS_INTERNAL_ERROR
} CmdStatusCode;

typedef enum {
    CMD_BODY_NONE,
    CMD_BODY_TEXT,
    CMD_BODY_JSON
} CmdBodyFormat;

typedef struct {
    char request_id[64];
    CmdRequestType type;
    char *sql;
} CmdRequest;

typedef struct {
    char request_id[64];
    CmdStatusCode status;
    int ok;
    int row_count;
    int affected_count;
    CmdBodyFormat body_format;
    char *body;
    size_t body_len;
    char *error_message;
} CmdResponse;

typedef struct {
    const char *name;
    size_t max_sql_len;
    size_t request_buffer_count;
    size_t response_body_capacity;
    void *shared_state;
} CmdProcessorContext;

struct CmdProcessor;

/*
 * Callback invoked when a submitted request completes.
 *
 * Both request and response remain processor-owned. The callback must release
 * them with cmd_processor_release_request() and
 * cmd_processor_release_response().
 */
typedef void (*CmdProcessorResponseCallback)(struct CmdProcessor *processor,
                                             CmdRequest *request,
                                             CmdResponse *response,
                                             void *user_data);

typedef struct CmdProcessor {
    CmdProcessorContext *context;
    int (*acquire_request)(CmdProcessorContext *context,
                           CmdRequest **out_request);
    int (*submit)(struct CmdProcessor *processor,
                  CmdProcessorContext *context,
                  CmdRequest *request,
                  CmdProcessorResponseCallback callback,
                  void *user_data);
    int (*make_error_response)(CmdProcessorContext *context,
                               const char *request_id,
                               CmdStatusCode status,
                               const char *error_message,
                               CmdResponse **out_response);
    void (*release_request)(CmdProcessorContext *context,
                            CmdRequest *request);
    void (*release_response)(CmdProcessorContext *context,
                             CmdResponse *response);
    void (*shutdown)(CmdProcessorContext *context);
} CmdProcessor;

const char *cmd_status_to_string(CmdStatusCode status);

int cmd_processor_acquire_request(CmdProcessor *processor,
                                  CmdRequest **out_request);

CmdStatusCode cmd_processor_set_sql_request(CmdProcessor *processor,
                                            CmdRequest *request,
                                            const char *request_id,
                                            const char *sql);

CmdStatusCode cmd_processor_set_ping_request(CmdProcessor *processor,
                                             CmdRequest *request,
                                             const char *request_id);

int cmd_processor_submit(CmdProcessor *processor,
                         CmdRequest *request,
                         CmdProcessorResponseCallback callback,
                         void *user_data);

int cmd_processor_make_error_response(CmdProcessor *processor,
                                      const char *request_id,
                                      CmdStatusCode status,
                                      const char *error_message,
                                      CmdResponse **out_response);

void cmd_processor_release_request(CmdProcessor *processor,
                                   CmdRequest *request);

void cmd_processor_release_response(CmdProcessor *processor,
                                    CmdResponse *response);

void cmd_processor_shutdown(CmdProcessor *processor);

#endif
