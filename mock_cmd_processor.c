#include "mock_cmd_processor.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MOCK_DEFAULT_MAX_SQL_LEN 4096u
#define MOCK_DEFAULT_BUFFER_COUNT 16u
#define MOCK_DEFAULT_BODY_CAPACITY 4096u

typedef struct {
    CmdRequest request;
    char *sql_buffer;
    int in_use;
} MockRequestSlot;

typedef struct {
    CmdResponse response;
    char *body_buffer;
    char *error_buffer;
    int in_use;
} MockResponseSlot;

typedef struct {
    CmdProcessor processor;
    CmdProcessorContext context;
    pthread_mutex_t mutex;
    MockRequestSlot *request_slots;
    MockResponseSlot *response_slots;
    size_t request_count;
    size_t response_count;
    size_t max_sql_len;
    size_t response_capacity;
} MockCmdProcessorState;

static void copy_fixed(char *dst, size_t dst_size, const char *src) {
    size_t len = 0;

    if (!dst || dst_size == 0) return;
    if (src) len = strlen(src);
    if (len >= dst_size) len = dst_size - 1;
    if (len > 0) memcpy(dst, src, len);
    dst[len] = '\0';
}

static void reset_request_slot(MockRequestSlot *slot) {
    if (!slot) return;
    memset(&slot->request, 0, sizeof(slot->request));
    slot->request.type = (CmdRequestType)-1;
    slot->request.sql = slot->sql_buffer;
    if (slot->sql_buffer) slot->sql_buffer[0] = '\0';
    slot->in_use = 0;
}

static void reset_response_slot(MockResponseSlot *slot) {
    if (!slot) return;
    memset(&slot->response, 0, sizeof(slot->response));
    slot->response.status = CMD_STATUS_INTERNAL_ERROR;
    slot->response.body_format = CMD_BODY_NONE;
    if (slot->body_buffer) slot->body_buffer[0] = '\0';
    if (slot->error_buffer) slot->error_buffer[0] = '\0';
    slot->in_use = 0;
}

static MockCmdProcessorState *state_from_context(CmdProcessorContext *context) {
    if (!context) return NULL;
    return (MockCmdProcessorState *)context->shared_state;
}

static int find_request_slot(MockCmdProcessorState *state, CmdRequest *request) {
    size_t i;

    if (!state || !request) return -1;
    for (i = 0; i < state->request_count; i++) {
        if (&state->request_slots[i].request == request) return (int)i;
    }
    return -1;
}

static int find_response_slot(MockCmdProcessorState *state, CmdResponse *response) {
    size_t i;

    if (!state || !response) return -1;
    for (i = 0; i < state->response_count; i++) {
        if (&state->response_slots[i].response == response) return (int)i;
    }
    return -1;
}

static MockResponseSlot *acquire_response_slot_locked(MockCmdProcessorState *state) {
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

static void init_response_common(MockResponseSlot *slot,
                                 const char *request_id,
                                 CmdStatusCode status,
                                 int ok) {
    CmdResponse *response;

    response = &slot->response;
    copy_fixed(response->request_id, sizeof(response->request_id), request_id);
    response->status = status;
    response->ok = ok;
    response->row_count = 0;
    response->affected_count = 0;
    response->body_format = CMD_BODY_NONE;
    response->body = NULL;
    response->body_len = 0;
    response->error_message = NULL;
}

static void set_error_response(MockResponseSlot *slot,
                               const char *request_id,
                               CmdStatusCode status,
                               const char *message,
                               size_t error_capacity) {
    init_response_common(slot, request_id, status, 0);
    if (message && slot->error_buffer) {
        copy_fixed(slot->error_buffer, error_capacity + 1, message);
        slot->response.error_message = slot->error_buffer;
    }
}

static int set_body_response(MockResponseSlot *slot,
                             const char *request_id,
                             CmdBodyFormat body_format,
                             const char *body,
                             size_t body_len,
                             size_t body_capacity) {
    if (!slot || !slot->body_buffer || !body || body_len > body_capacity) {
        return 0;
    }

    init_response_common(slot, request_id, CMD_STATUS_OK, 1);
    memmove(slot->body_buffer, body, body_len);
    slot->body_buffer[body_len] = '\0';
    slot->response.body_format = body_format;
    slot->response.body = slot->body_buffer;
    slot->response.body_len = body_len;
    return 1;
}

static int append_bytes(char *dst, size_t dst_size, size_t *pos, const char *src, size_t len) {
    if (!dst || !pos || !src || *pos + len >= dst_size) return 0;
    memcpy(dst + *pos, src, len);
    *pos += len;
    dst[*pos] = '\0';
    return 1;
}

static int append_cstr(char *dst, size_t dst_size, size_t *pos, const char *src) {
    return append_bytes(dst, dst_size, pos, src, strlen(src));
}

static int append_char(char *dst, size_t dst_size, size_t *pos, char ch) {
    return append_bytes(dst, dst_size, pos, &ch, 1);
}

static int append_json_escaped(char *dst, size_t dst_size, size_t *pos, const char *src) {
    const unsigned char *cursor = (const unsigned char *)src;
    char encoded[7];

    while (*cursor) {
        switch (*cursor) {
            case '\"':
                if (!append_cstr(dst, dst_size, pos, "\\\"")) return 0;
                break;
            case '\\':
                if (!append_cstr(dst, dst_size, pos, "\\\\")) return 0;
                break;
            case '\b':
                if (!append_cstr(dst, dst_size, pos, "\\b")) return 0;
                break;
            case '\f':
                if (!append_cstr(dst, dst_size, pos, "\\f")) return 0;
                break;
            case '\n':
                if (!append_cstr(dst, dst_size, pos, "\\n")) return 0;
                break;
            case '\r':
                if (!append_cstr(dst, dst_size, pos, "\\r")) return 0;
                break;
            case '\t':
                if (!append_cstr(dst, dst_size, pos, "\\t")) return 0;
                break;
            default:
                if (*cursor < 0x20) {
                    snprintf(encoded, sizeof(encoded), "\\u%04x", (unsigned int)*cursor);
                    if (!append_cstr(dst, dst_size, pos, encoded)) return 0;
                } else if (!append_char(dst, dst_size, pos, (char)*cursor)) {
                    return 0;
                }
                break;
        }
        cursor++;
    }
    return 1;
}

static int build_sql_echo_json(char *dst, size_t dst_size, const char *sql, size_t *out_len) {
    size_t pos = 0;

    if (!dst || dst_size == 0 || !out_len) return 0;
    dst[0] = '\0';
    if (!append_cstr(dst, dst_size, &pos, "{\"mock\":true,\"sql\":\"")) return 0;
    if (!append_json_escaped(dst, dst_size, &pos, sql ? sql : "")) return 0;
    if (!append_cstr(dst, dst_size, &pos, "\"}")) return 0;
    *out_len = pos;
    return 1;
}

static int mock_acquire_request(CmdProcessorContext *context,
                                CmdRequest **out_request) {
    MockCmdProcessorState *state;
    size_t i;

    if (out_request) *out_request = NULL;
    state = state_from_context(context);
    if (!state || !out_request) return -1;

    pthread_mutex_lock(&state->mutex);
    for (i = 0; i < state->request_count; i++) {
        if (!state->request_slots[i].in_use) {
            reset_request_slot(&state->request_slots[i]);
            state->request_slots[i].in_use = 1;
            *out_request = &state->request_slots[i].request;
            pthread_mutex_unlock(&state->mutex);
            return 0;
        }
    }
    pthread_mutex_unlock(&state->mutex);
    return -1;
}

static int mock_process(CmdProcessorContext *context,
                        CmdRequest *request,
                        CmdResponse **out_response) {
    MockCmdProcessorState *state;
    MockResponseSlot *slot;
    int request_index;
    char echo_stack[512];
    char *echo;
    size_t echo_len = 0;
    int echo_ok;

    if (out_response) *out_response = NULL;
    state = state_from_context(context);
    if (!state || !request || !out_response) return -1;

    pthread_mutex_lock(&state->mutex);
    request_index = find_request_slot(state, request);
    if (request_index < 0 || !state->request_slots[request_index].in_use) {
        pthread_mutex_unlock(&state->mutex);
        return -1;
    }
    slot = acquire_response_slot_locked(state);
    pthread_mutex_unlock(&state->mutex);
    if (!slot) return -1;

    if (request->type == CMD_REQUEST_PING) {
        if (!set_body_response(slot,
                               request->request_id,
                               CMD_BODY_TEXT,
                               "pong",
                               4,
                               state->response_capacity)) {
            set_error_response(slot,
                               request->request_id,
                               CMD_STATUS_PROCESSING_ERROR,
                               "response body capacity exceeded",
                               state->response_capacity);
        }
        *out_response = &slot->response;
        return 0;
    }

    if (request->type != CMD_REQUEST_SQL || !request->sql) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_BAD_REQUEST,
                           "unsupported mock request",
                           state->response_capacity);
        *out_response = &slot->response;
        return 0;
    }

    if (strcmp(request->sql, "MOCK_BUSY") == 0) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_BUSY,
                           "mock busy",
                           state->response_capacity);
        *out_response = &slot->response;
        return 0;
    }
    if (strcmp(request->sql, "MOCK_TIMEOUT") == 0) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_TIMEOUT,
                           "mock timeout",
                           state->response_capacity);
        *out_response = &slot->response;
        return 0;
    }
    if (strcmp(request->sql, "MOCK_PROCESSING_ERROR") == 0) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_PROCESSING_ERROR,
                           "mock processing error",
                           state->response_capacity);
        *out_response = &slot->response;
        return 0;
    }

    echo = slot->body_buffer ? slot->body_buffer : echo_stack;
    echo_ok = build_sql_echo_json(echo,
                                  state->response_capacity + 1,
                                  request->sql,
                                  &echo_len);
    if (!echo_ok) {
        set_error_response(slot,
                           request->request_id,
                           CMD_STATUS_PROCESSING_ERROR,
                           "response body capacity exceeded",
                           state->response_capacity);
        *out_response = &slot->response;
        return 0;
    }

    set_body_response(slot,
                      request->request_id,
                      CMD_BODY_JSON,
                      echo,
                      echo_len,
                      state->response_capacity);
    *out_response = &slot->response;
    return 0;
}

static int mock_make_error_response(CmdProcessorContext *context,
                                    const char *request_id,
                                    CmdStatusCode status,
                                    const char *error_message,
                                    CmdResponse **out_response) {
    MockCmdProcessorState *state;
    MockResponseSlot *slot;

    if (out_response) *out_response = NULL;
    state = state_from_context(context);
    if (!state || !out_response) return -1;

    pthread_mutex_lock(&state->mutex);
    slot = acquire_response_slot_locked(state);
    pthread_mutex_unlock(&state->mutex);
    if (!slot) return -1;

    set_error_response(slot, request_id, status, error_message, state->response_capacity);
    *out_response = &slot->response;
    return 0;
}

static void mock_release_request(CmdProcessorContext *context,
                                 CmdRequest *request) {
    MockCmdProcessorState *state;
    int index;

    state = state_from_context(context);
    if (!state || !request) return;

    pthread_mutex_lock(&state->mutex);
    index = find_request_slot(state, request);
    if (index >= 0) reset_request_slot(&state->request_slots[index]);
    pthread_mutex_unlock(&state->mutex);
}

static void mock_release_response(CmdProcessorContext *context,
                                  CmdResponse *response) {
    MockCmdProcessorState *state;
    int index;

    state = state_from_context(context);
    if (!state || !response) return;

    pthread_mutex_lock(&state->mutex);
    index = find_response_slot(state, response);
    if (index >= 0) reset_response_slot(&state->response_slots[index]);
    pthread_mutex_unlock(&state->mutex);
}

static void mock_shutdown(CmdProcessorContext *context) {
    MockCmdProcessorState *state;
    size_t i;

    state = state_from_context(context);
    if (!state) return;

    pthread_mutex_destroy(&state->mutex);
    if (state->request_slots) {
        for (i = 0; i < state->request_count; i++) {
            free(state->request_slots[i].sql_buffer);
        }
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

static void free_partial_state(MockCmdProcessorState *state) {
    if (!state) return;
    state->context.shared_state = state;
    mock_shutdown(&state->context);
}

int mock_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor) {
    MockCmdProcessorState *state;
    size_t i;

    if (out_processor) *out_processor = NULL;
    if (!out_processor) return -1;

    state = (MockCmdProcessorState *)calloc(1, sizeof(*state));
    if (!state) return -1;

    state->max_sql_len = base_context && base_context->max_sql_len
                             ? base_context->max_sql_len
                             : MOCK_DEFAULT_MAX_SQL_LEN;
    state->request_count = base_context && base_context->request_buffer_count
                               ? base_context->request_buffer_count
                               : MOCK_DEFAULT_BUFFER_COUNT;
    state->response_count = state->request_count;
    state->response_capacity = base_context && base_context->response_body_capacity
                                   ? base_context->response_body_capacity
                                   : MOCK_DEFAULT_BODY_CAPACITY;

    if (state->max_sql_len == SIZE_MAX || state->response_capacity == SIZE_MAX) {
        free(state);
        return -1;
    }

    if (pthread_mutex_init(&state->mutex, NULL) != 0) {
        free(state);
        return -1;
    }

    state->request_slots = (MockRequestSlot *)calloc(state->request_count,
                                                     sizeof(*state->request_slots));
    state->response_slots = (MockResponseSlot *)calloc(state->response_count,
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
        state->response_slots[i].error_buffer = (char *)calloc(state->response_capacity + 1, 1);
        if (!state->response_slots[i].body_buffer || !state->response_slots[i].error_buffer) {
            free_partial_state(state);
            return -1;
        }
        reset_response_slot(&state->response_slots[i]);
    }

    state->context.name = base_context && base_context->name
                              ? base_context->name
                              : "mock_cmd_processor";
    state->context.max_sql_len = state->max_sql_len;
    state->context.request_buffer_count = state->request_count;
    state->context.response_body_capacity = state->response_capacity;
    state->context.shared_state = state;

    state->processor.context = &state->context;
    state->processor.acquire_request = mock_acquire_request;
    state->processor.process = mock_process;
    state->processor.make_error_response = mock_make_error_response;
    state->processor.release_request = mock_release_request;
    state->processor.release_response = mock_release_response;
    state->processor.shutdown = mock_shutdown;

    *out_processor = &state->processor;
    return 0;
}
