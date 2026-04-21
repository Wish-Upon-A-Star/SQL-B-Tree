#include "cmd_processor.h"

#include <string.h>

static int processor_is_usable(CmdProcessor *processor) {
    return processor && processor->context;
}

static void copy_request_id(char request_id[64], const char *source) {
    size_t len = 0;

    if (!request_id) return;
    if (source) len = strlen(source);
    if (len >= 64) len = 63;
    if (len > 0) memcpy(request_id, source, len);
    request_id[len] = '\0';
}

const char *cmd_status_to_string(CmdStatusCode status) {
    switch (status) {
        case CMD_STATUS_OK:
            return "OK";
        case CMD_STATUS_BAD_REQUEST:
            return "BAD_REQUEST";
        case CMD_STATUS_SQL_TOO_LONG:
            return "SQL_TOO_LONG";
        case CMD_STATUS_PARSE_ERROR:
            return "PARSE_ERROR";
        case CMD_STATUS_PROCESSING_ERROR:
            return "PROCESSING_ERROR";
        case CMD_STATUS_BUSY:
            return "BUSY";
        case CMD_STATUS_TIMEOUT:
            return "TIMEOUT";
        case CMD_STATUS_INTERNAL_ERROR:
            return "INTERNAL_ERROR";
        default:
            return "UNKNOWN";
    }
}

int cmd_processor_acquire_request(CmdProcessor *processor,
                                  CmdRequest **out_request) {
    if (out_request) *out_request = NULL;
    if (!processor_is_usable(processor) || !processor->acquire_request || !out_request) {
        return -1;
    }

    return processor->acquire_request(processor->context, out_request);
}

CmdStatusCode cmd_processor_set_sql_request(CmdProcessor *processor,
                                            CmdRequest *request,
                                            const char *request_id,
                                            const char *sql) {
    size_t sql_len;

    if (!processor_is_usable(processor) || !request || !request->sql || !sql) {
        return CMD_STATUS_BAD_REQUEST;
    }

    sql_len = strlen(sql);
    if (sql_len > processor->context->max_sql_len) {
        return CMD_STATUS_SQL_TOO_LONG;
    }

    copy_request_id(request->request_id, request_id);
    request->type = CMD_REQUEST_SQL;
    memcpy(request->sql, sql, sql_len + 1);
    return CMD_STATUS_OK;
}

CmdStatusCode cmd_processor_set_ping_request(CmdProcessor *processor,
                                             CmdRequest *request,
                                             const char *request_id) {
    if (!processor_is_usable(processor) || !request) {
        return CMD_STATUS_BAD_REQUEST;
    }

    copy_request_id(request->request_id, request_id);
    request->type = CMD_REQUEST_PING;
    if (request->sql) request->sql[0] = '\0';
    return CMD_STATUS_OK;
}

int cmd_processor_submit(CmdProcessor *processor,
                         CmdRequest *request,
                         CmdProcessorResponseCallback callback,
                         void *user_data) {
    if (!processor_is_usable(processor) || !processor->submit || !request || !callback) {
        return -1;
    }

    return processor->submit(processor,
                             processor->context,
                             request,
                             callback,
                             user_data);
}

int cmd_processor_make_error_response(CmdProcessor *processor,
                                      const char *request_id,
                                      CmdStatusCode status,
                                      const char *error_message,
                                      CmdResponse **out_response) {
    if (out_response) *out_response = NULL;
    if (!processor_is_usable(processor) || !processor->make_error_response || !out_response) {
        return -1;
    }

    return processor->make_error_response(processor->context,
                                          request_id,
                                          status,
                                          error_message,
                                          out_response);
}

void cmd_processor_release_request(CmdProcessor *processor,
                                   CmdRequest *request) {
    if (!processor_is_usable(processor) || !processor->release_request || !request) {
        return;
    }

    processor->release_request(processor->context, request);
}

void cmd_processor_release_response(CmdProcessor *processor,
                                    CmdResponse *response) {
    if (!processor_is_usable(processor) || !processor->release_response || !response) {
        return;
    }

    processor->release_response(processor->context, response);
}

void cmd_processor_shutdown(CmdProcessor *processor) {
    if (!processor_is_usable(processor) || !processor->shutdown) {
        return;
    }

    processor->shutdown(processor->context);
}
