#include "repl_cmd_processor.h"

#include "sql_repl_engine.h"

#include "../executor.h"
#include "../platform_threads.h"
#include "../types.h"

#include <stdlib.h>
#include <string.h>

#define REPL_DEFAULT_NAME "repl_cmd_processor"
#define REPL_DEFAULT_BUFFER_COUNT 1u
#define REPL_DEFAULT_BODY_CAPACITY 8192u
#define REPL_ERROR_CAPACITY 256u

typedef struct {
    CmdRequest request;
    char *sql_buffer;
    int in_use;
} REPLRequestSlot;

typedef struct {
    CmdResponse response;
    char *body_buffer;
    char *error_buffer;
    int in_use;
} REPLResponseSlot;

typedef struct {
    CmdProcessor processor;
    CmdProcessorContext context;
    db_mutex_t mutex;
    REPLRequestSlot *request_slots;
    REPLResponseSlot *response_slots;
    size_t request_count;
    size_t response_count;
    size_t max_sql_len;
    size_t response_capacity;
    size_t error_capacity;
} REPLCmdProcessorState;

static void copy_fixed(char *dst, size_t dst_size, const char *src) {
    size_t len = 0;

    if (!dst || dst_size == 0) return;
    if (src) len = strlen(src);
    if (len >= dst_size) len = dst_size - 1;
    if (len > 0) memcpy(dst, src, len);
    dst[len] = '\0';
}

static REPLCmdProcessorState *state_from_context(CmdProcessorContext *context) {
    if (!context) return NULL;
    return (REPLCmdProcessorState *)context->shared_state;
}

static void reset_request_slot(REPLRequestSlot *slot) {
    if (!slot) return;
    memset(&slot->request, 0, sizeof(slot->request));
    slot->request.type = (CmdRequestType)-1;
    slot->request.sql = slot->sql_buffer;
    if (slot->sql_buffer) slot->sql_buffer[0] = '\0';
    slot->in_use = 0;
}

static void reset_response_slot(REPLResponseSlot *slot) {
    if (!slot) return;
    memset(&slot->response, 0, sizeof(slot->response));
    slot->response.status = CMD_STATUS_INTERNAL_ERROR;
    slot->response.body_format = CMD_BODY_NONE;
    if (slot->body_buffer) slot->body_buffer[0] = '\0';
    if (slot->error_buffer) slot->error_buffer[0] = '\0';
    slot->in_use = 0;
}

static int find_request_slot(REPLCmdProcessorState *state, CmdRequest *request) {
    size_t i;

    if (!state || !request) return -1;
    for (i = 0; i < state->request_count; i++) {
        if (&state->request_slots[i].request == request) return (int)i;
    }
    return -1;
}

static int find_response_slot(REPLCmdProcessorState *state, CmdResponse *response) {
    size_t i;

    if (!state || !response) return -1;
    for (i = 0; i < state->response_count; i++) {
        if (&state->response_slots[i].response == response) return (int)i;
    }
    return -1;
}

static REPLResponseSlot *acquire_response_slot_locked(REPLCmdProcessorState *state) {
    size_t i;

    if (!state) return NULL;
    for (i = 0; i < state->response_count; i++) {
        if (!state->response_slots[i].in_use) {
            reset_response_slot(&state->response_slots[i]);
            state->response_slots[i].in_use = 1;
            return &state->response_slots[i];
        }
    }
    return NULL;
}

static void init_response_common(REPLResponseSlot *slot,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 int ok) {
    CmdResponse *response;

    if (!slot) return;
    response = &slot->response;
    memset(response, 0, sizeof(*response));
    copy_fixed(response->request_id, sizeof(response->request_id), request_id);
    response->status = status;
    response->ok = ok;
    response->body_format = CMD_BODY_NONE;
}

static void set_error_response(REPLResponseSlot *slot,
                               const char *request_id,
                               CmdStatusCode status,
                               const char *message,
                               size_t error_capacity) {
    init_response_common(slot, request_id, status, 0);
    if (message && slot && slot->error_buffer) {
        copy_fixed(slot->error_buffer, error_capacity + 1, message);
        slot->response.error_message = slot->error_buffer;
    }
}

static void set_text_response(REPLResponseSlot *slot,
                              const char *request_id,
                              const char *body,
                              size_t body_len,
                              size_t body_capacity) {
    init_response_common(slot, request_id, CMD_STATUS_OK, 1);
    if (!slot || !slot->body_buffer) return;
    if (!body) body = "";
    if (body_len > body_capacity) body_len = body_capacity;
    if (body != slot->body_buffer) memcpy(slot->body_buffer, body, body_len);
    slot->body_buffer[body_len] = '\0';
    slot->response.body_format = CMD_BODY_TEXT;
    slot->response.body = slot->body_buffer;
    slot->response.body_len = body_len;
}

static int repl_acquire_request(CmdProcessorContext *context,
                                CmdRequest **out_request) {
    REPLCmdProcessorState *state;
    size_t i;

    if (out_request) *out_request = NULL;
    state = state_from_context(context);
    if (!state || !out_request) return -1;

    db_mutex_lock(&state->mutex);
    for (i = 0; i < state->request_count; i++) {
        if (!state->request_slots[i].in_use) {
            reset_request_slot(&state->request_slots[i]);
            state->request_slots[i].in_use = 1;
            *out_request = &state->request_slots[i].request;
            db_mutex_unlock(&state->mutex);
            return 0;
        }
    }
    db_mutex_unlock(&state->mutex);
    return -1;
}

static void fill_sql_response(REPLCmdProcessorState *state,
                              CmdRequest *request,
                              REPLResponseSlot *slot) {
    SqlOutputBuffer output;
    SqlEvalResult result;

    sql_output_buffer_init(&output, slot->body_buffer, state->response_capacity);
    result = sql_repl_eval(request->sql, &output);
    if (output.truncated) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_PROCESSING_ERROR,
                           "response body capacity exceeded",
                           state->error_capacity);
        return;
    }

    if (result.ok) {
        set_text_response(slot,
                          request->request_id,
                          slot->body_buffer,
                          output.len,
                          state->response_capacity);
        slot->response.row_count = result.row_count;
        slot->response.affected_count = result.affected_count;
        return;
    }

    set_error_response(slot,
                       request->request_id,
                       result.status,
                       slot->body_buffer && slot->body_buffer[0] ? slot->body_buffer : "request failed",
                       state->error_capacity);
}

static int repl_submit(CmdProcessor *processor,
                       CmdProcessorContext *context,
                       CmdRequest *request,
                       CmdProcessorResponseCallback callback,
                       void *user_data) {
    REPLCmdProcessorState *state;
    REPLResponseSlot *slot;
    int request_index;

    state = state_from_context(context);
    if (!processor || !state || !request || !callback) return -1;

    db_mutex_lock(&state->mutex);
    request_index = find_request_slot(state, request);
    if (request_index < 0 || !state->request_slots[request_index].in_use) {
        db_mutex_unlock(&state->mutex);
        return -1;
    }

    slot = acquire_response_slot_locked(state);
    if (!slot) {
        db_mutex_unlock(&state->mutex);
        return -1;
    }

    if (request->type == CMD_REQUEST_PING) {
        set_text_response(slot,
                          request->request_id,
                          "pong",
                          4,
                          state->response_capacity);
    } else if (request->type == CMD_REQUEST_SQL && request->sql) {
        fill_sql_response(state, request, slot);
    } else {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_BAD_REQUEST,
                           "unsupported REPL request",
                           state->error_capacity);
    }
    db_mutex_unlock(&state->mutex);

    callback(processor, request, &slot->response, user_data);
    return 0;
}

static int repl_make_error_response(CmdProcessorContext *context,
                                    const char *request_id,
                                    CmdStatusCode status,
                                    const char *error_message,
                                    CmdResponse **out_response) {
    REPLCmdProcessorState *state;
    REPLResponseSlot *slot;

    if (out_response) *out_response = NULL;
    state = state_from_context(context);
    if (!state || !out_response) return -1;

    db_mutex_lock(&state->mutex);
    slot = acquire_response_slot_locked(state);
    if (slot) {
        set_error_response(slot, request_id, status, error_message, state->error_capacity);
        *out_response = &slot->response;
    }
    db_mutex_unlock(&state->mutex);
    return slot ? 0 : -1;
}

static void repl_release_request(CmdProcessorContext *context,
                                 CmdRequest *request) {
    REPLCmdProcessorState *state;
    int index;

    state = state_from_context(context);
    if (!state || !request) return;

    db_mutex_lock(&state->mutex);
    index = find_request_slot(state, request);
    if (index >= 0) reset_request_slot(&state->request_slots[index]);
    db_mutex_unlock(&state->mutex);
}

static void repl_release_response(CmdProcessorContext *context,
                                  CmdResponse *response) {
    REPLCmdProcessorState *state;
    int index;

    state = state_from_context(context);
    if (!state || !response) return;

    db_mutex_lock(&state->mutex);
    index = find_response_slot(state, response);
    if (index >= 0) reset_response_slot(&state->response_slots[index]);
    db_mutex_unlock(&state->mutex);
}

static void repl_shutdown(CmdProcessorContext *context) {
    REPLCmdProcessorState *state;
    size_t i;

    state = state_from_context(context);
    if (!state) return;

    close_all_tables();
    db_mutex_destroy(&state->mutex);
    if (state->request_slots) {
        for (i = 0; i < state->request_count; i++) free(state->request_slots[i].sql_buffer);
    }
    if (state->response_slots) {
        for (i = 0; i < state->response_count; i++) {
            free(state->response_slots[i].body_buffer);
            free(state->response_slots[i].error_buffer);
        }
    }
    free(state->request_slots);
    free(state->response_slots);
    free(state);
}

static void free_partial_state(REPLCmdProcessorState *state) {
    if (!state) return;
    state->context.shared_state = state;
    repl_shutdown(&state->context);
}

int repl_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor) {
    REPLCmdProcessorState *state;
    size_t i;

    if (out_processor) *out_processor = NULL;
    if (!out_processor) return -1;

    state = (REPLCmdProcessorState *)calloc(1, sizeof(*state));
    if (!state) return -1;

    state->max_sql_len = base_context && base_context->max_sql_len
                             ? base_context->max_sql_len
                             : (MAX_SQL_LEN - 1);
    state->request_count = base_context && base_context->request_buffer_count
                               ? base_context->request_buffer_count
                               : REPL_DEFAULT_BUFFER_COUNT;
    state->response_count = state->request_count;
    state->response_capacity = base_context && base_context->response_body_capacity
                                   ? base_context->response_body_capacity
                                   : REPL_DEFAULT_BODY_CAPACITY;
    state->error_capacity = REPL_ERROR_CAPACITY;

    if (db_mutex_init(&state->mutex) != 1) {
        free(state);
        return -1;
    }

    state->request_slots = (REPLRequestSlot *)calloc(state->request_count,
                                                     sizeof(*state->request_slots));
    state->response_slots = (REPLResponseSlot *)calloc(state->response_count,
                                                       sizeof(*state->response_slots));
    if (!state->request_slots || !state->response_slots) {
        free_partial_state(state);
        return -1;
    }

    for (i = 0; i < state->request_count; i++) {
        state->request_slots[i].sql_buffer = (char *)calloc(state->max_sql_len + 1, 1);
        if (!state->request_slots[i].sql_buffer) {
            free_partial_state(state);
            return -1;
        }
        reset_request_slot(&state->request_slots[i]);
    }

    for (i = 0; i < state->response_count; i++) {
        state->response_slots[i].body_buffer = (char *)calloc(state->response_capacity + 1, 1);
        state->response_slots[i].error_buffer = (char *)calloc(state->error_capacity + 1, 1);
        if (!state->response_slots[i].body_buffer || !state->response_slots[i].error_buffer) {
            free_partial_state(state);
            return -1;
        }
        reset_response_slot(&state->response_slots[i]);
    }

    state->context.name = base_context && base_context->name
                              ? base_context->name
                              : REPL_DEFAULT_NAME;
    state->context.max_sql_len = state->max_sql_len;
    state->context.request_buffer_count = state->request_count;
    state->context.response_body_capacity = state->response_capacity;
    state->context.shared_state = state;

    state->processor.context = &state->context;
    state->processor.acquire_request = repl_acquire_request;
    state->processor.submit = repl_submit;
    state->processor.make_error_response = repl_make_error_response;
    state->processor.release_request = repl_release_request;
    state->processor.release_response = repl_release_response;
    state->processor.shutdown = repl_shutdown;

    *out_processor = &state->processor;
    return 0;
}
