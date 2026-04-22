#include "engine_cmd_processor_internal.h"

static void release_lock_handles(const LockPlan *plan, LockHandle *handles) {
    int i;

    if (!plan || !handles) return;
    for (i = plan->lock_count - 1; i >= 0; i--) {
        if (!handles[i].acquired || !handles[i].bucket) continue;
        if (handles[i].mode == ENGINE_LOCK_WRITE) db_rwlock_wrunlock(&handles[i].bucket->rwlock);
        else db_rwlock_rdunlock(&handles[i].bucket->rwlock);
    }
}

static void release_partial_lock_handles(LockHandle *handles, int last_index) {
    while (last_index >= 0) {
        if (handles[last_index].acquired && handles[last_index].bucket) {
            if (handles[last_index].mode == ENGINE_LOCK_WRITE) db_rwlock_wrunlock(&handles[last_index].bucket->rwlock);
            else db_rwlock_rdunlock(&handles[last_index].bucket->rwlock);
        }
        last_index--;
    }
}

static void set_invalid_sql_request_error(EngineCmdProcessorState *state,
                                          CmdRequest *request,
                                          ResponseSlot *response_slot) {
    set_response_error(response_slot,
                       request->request_id,
                       CMD_STATUS_BAD_REQUEST,
                       "sql request is required",
                       state->response_body_capacity);
}

static int parse_statement_or_respond(EngineCmdProcessorState *state,
                                      CmdRequest *request,
                                      ResponseSlot *response_slot,
                                      Statement *stmt) {
    if (parse_statement(request->sql, stmt)) return 1;

    set_response_error(response_slot,
                       request->request_id,
                       CMD_STATUS_PARSE_ERROR,
                       "parse failed",
                       state->response_body_capacity);
    return 0;
}

static int execute_statement_or_respond(EngineCmdProcessorState *state,
                                        CmdRequest *request,
                                        ResponseSlot *response_slot,
                                        Statement *stmt,
                                        ExecutorResult *result) {
    if (executor_execute_statement_with_result(stmt, result)) return 1;

    set_response_error(response_slot,
                       request->request_id,
                       CMD_STATUS_PROCESSING_ERROR,
                       "execution failed",
                       state->response_body_capacity);
    return 0;
}

typedef struct {
    unsigned char *data;
    size_t size;
    size_t used;
} BinaryBuffer;

static int binary_buffer_append(BinaryBuffer *buffer, const void *data, size_t data_size) {
    if (!buffer || !data) return 0;
    if (buffer->used + data_size > buffer->size) return 0;
    memcpy(buffer->data + buffer->used, data, data_size);
    buffer->used += data_size;
    return 1;
}

static int binary_buffer_append_u16(BinaryBuffer *buffer, uint16_t value) {
    unsigned char bytes[2];
    bytes[0] = (unsigned char)(value & 0xffu);
    bytes[1] = (unsigned char)((value >> 8) & 0xffu);
    return binary_buffer_append(buffer, bytes, sizeof(bytes));
}

static int binary_buffer_append_u32(BinaryBuffer *buffer, uint32_t value) {
    unsigned char bytes[4];
    bytes[0] = (unsigned char)(value & 0xffu);
    bytes[1] = (unsigned char)((value >> 8) & 0xffu);
    bytes[2] = (unsigned char)((value >> 16) & 0xffu);
    bytes[3] = (unsigned char)((value >> 24) & 0xffu);
    return binary_buffer_append(buffer, bytes, sizeof(bytes));
}

static int binary_buffer_append_text(BinaryBuffer *buffer, const char *text, size_t text_len) {
    if (!binary_buffer_append_u32(buffer, (uint32_t)text_len)) return 0;
    if (text_len == 0) return 1;
    return binary_buffer_append(buffer, text, text_len);
}

static size_t estimate_select_binary_size(const ExecutorResult *result) {
    size_t total = 16;
    int row_index;
    int col_index;

    if (!result) return 0;
    for (col_index = 0; col_index < result->select_column_count; col_index++) {
        total += 2 + strlen(result->select_columns[col_index]);
    }
    for (row_index = 0; row_index < result->select_row_count; row_index++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};

        parse_csv_row(result->select_rows[row_index], fields, row_buf);
        for (col_index = 0; col_index < result->select_column_count; col_index++) {
            const char *field = fields[result->select_column_indices[col_index]];
            total += 4 + strlen(field ? field : "");
        }
    }
    return total;
}

static int build_select_result_binary(const ExecutorResult *result,
                                      unsigned char *buffer,
                                      size_t buffer_size,
                                      size_t *out_size) {
    BinaryBuffer writer;
    int row_index;
    int col_index;

    if (out_size) *out_size = 0;
    if (!result || !buffer || buffer_size == 0 || !out_size) return 0;

    writer.data = buffer;
    writer.size = buffer_size;
    writer.used = 0;

    if (!binary_buffer_append_u32(&writer, 0x31524442u) ||
        !binary_buffer_append_u16(&writer, 1u) ||
        !binary_buffer_append_u16(&writer, 0u) ||
        !binary_buffer_append_u32(&writer, (uint32_t)result->select_row_count) ||
        !binary_buffer_append_u16(&writer, (uint16_t)result->select_column_count) ||
        !binary_buffer_append_u16(&writer, 0u)) {
        return 0;
    }

    for (col_index = 0; col_index < result->select_column_count; col_index++) {
        size_t name_len = strlen(result->select_columns[col_index]);
        if (name_len > 0xffffu) return 0;
        if (!binary_buffer_append_u16(&writer, (uint16_t)name_len) ||
            (name_len > 0 && !binary_buffer_append(&writer, result->select_columns[col_index], name_len))) {
            return 0;
        }
    }

    for (row_index = 0; row_index < result->select_row_count; row_index++) {
        char row_buf[RECORD_SIZE];
        char *fields[MAX_COLS] = {0};

        parse_csv_row(result->select_rows[row_index], fields, row_buf);
        for (col_index = 0; col_index < result->select_column_count; col_index++) {
            const char *field = fields[result->select_column_indices[col_index]];
            size_t field_len = strlen(field ? field : "");
            if (!binary_buffer_append_text(&writer, field ? field : "", field_len)) return 0;
        }
    }

    *out_size = writer.used;
    return 1;
}

static void set_statement_success_response(EngineCmdProcessorState *state,
                                           CmdRequest *request,
                                           ResponseSlot *response_slot,
                                           const Statement *stmt,
                                           const ExecutorResult *result) {
    char body[256];
    size_t binary_size = 0;
    size_t required_size;

    if (!stmt || !result) return;

    if (stmt->type == STMT_SELECT) {
        required_size = estimate_select_binary_size(result);
        if (required_size == 0 || required_size > state->response_body_capacity) {
            set_response_error(response_slot,
                               request->request_id,
                               CMD_STATUS_PROCESSING_ERROR,
                               "response too large",
                               state->response_body_capacity);
            return;
        }
        if (!build_select_result_binary(result,
                                        (unsigned char *)response_slot->message_buffer,
                                        state->response_body_capacity,
                                        &binary_size)) {
            set_response_error(response_slot,
                               request->request_id,
                               CMD_STATUS_INTERNAL_ERROR,
                               "response serialization failed",
                               state->response_body_capacity);
            return;
        }
        set_response_body(response_slot,
                          request->request_id,
                          CMD_BODY_BINARY,
                          response_slot->message_buffer,
                          binary_size,
                          state->response_body_capacity);
        response_slot->response.row_count = result->matched_rows;
        response_slot->response.affected_count = result->affected_rows;
        return;
    } else if (stmt->type == STMT_INSERT) {
        snprintf(body, sizeof(body), "INSERT affected_rows=%d id=%ld", result->affected_rows, result->generated_id);
    } else {
        snprintf(body,
                 sizeof(body),
                 "%s affected_rows=%d",
                 stmt->type == STMT_UPDATE ? "UPDATE" : "DELETE",
                 result->affected_rows);
    }

    set_response_body(response_slot,
                      request->request_id,
                      CMD_BODY_TEXT,
                      body,
                      strlen(body),
                      state->response_body_capacity);
    response_slot->response.row_count = result->matched_rows;
    response_slot->response.affected_count = result->affected_rows;
}

static void record_completed_job_metrics(EngineCmdProcessorState *state, CmdJob *job) {
    job->stats.total_us = job->stats.queue_wait_us + job->stats.lock_wait_us + job->stats.exec_us;
    metrics_record(&state->metrics,
                   &job->stats,
                   job->response_slot->response.ok,
                   job->response_slot->response.status,
                   job->enqueue_depth);
}

int lock_manager_init(LockManager *manager) {

    if (!manager) return 0;
    memset(manager, 0, sizeof(*manager));
    return db_mutex_init(&manager->mutex);
}

void lock_manager_destroy(LockManager *manager) {
    int i;
    if (!manager) return;
    for (i = 0; i < ENGINE_LOCK_BUCKETS; i++) {
        if (manager->buckets[i].in_use) db_rwlock_destroy(&manager->buckets[i].rwlock);
    }
    db_mutex_destroy(&manager->mutex);
    memset(manager, 0, sizeof(*manager));
}

static LockBucket *find_or_create_bucket(LockManager *manager, const char *name) {
    int slot = -1;
    int i;
    unsigned long start = hash_text(name) % ENGINE_LOCK_BUCKETS;
    for (i = 0; i < ENGINE_LOCK_BUCKETS; i++) {
        int idx = (int)((start + (unsigned long)i) % ENGINE_LOCK_BUCKETS);
        if (manager->buckets[idx].in_use) {
            if (strcmp(manager->buckets[idx].name, name) == 0) return &manager->buckets[idx];
            continue;
        }
        if (slot < 0) slot = idx;
    }
    if (slot < 0) return NULL;
    copy_fixed(manager->buckets[slot].name, sizeof(manager->buckets[slot].name), name);
    if (!db_rwlock_init(&manager->buckets[slot].rwlock)) return NULL;
    manager->buckets[slot].in_use = 1;
    return &manager->buckets[slot];
}

int lock_manager_acquire(LockManager *manager, const LockPlan *plan, LockHandle *handles, unsigned long long *wait_us) {
    int i;
    if (wait_us) *wait_us = 0;
    if (!manager || !plan || !handles) return 0;
    for (i = 0; i < plan->lock_count; i++) {
        uint64_t start_us;
        db_mutex_lock(&manager->mutex);
        handles[i].bucket = find_or_create_bucket(manager, plan->targets[i].name);
        db_mutex_unlock(&manager->mutex);
        if (!handles[i].bucket) {
            release_partial_lock_handles(handles, i - 1);
            return 0;
        }
        handles[i].mode = plan->targets[i].mode;
        start_us = monotonic_us();
        if (handles[i].mode == ENGINE_LOCK_WRITE) db_rwlock_wrlock(&handles[i].bucket->rwlock);
        else db_rwlock_rdlock(&handles[i].bucket->rwlock);
        handles[i].wait_us = monotonic_us() - start_us;
        handles[i].acquired = 1;
        if (wait_us) *wait_us += handles[i].wait_us;
    }
    return 1;
}

void lock_manager_release(LockManager *manager, const LockPlan *plan, LockHandle *handles) {
    (void)manager;
    release_lock_handles(plan, handles);
}

int metrics_init(MetricsSink *sink) {
    if (!sink) return 0;
    memset(sink, 0, sizeof(*sink));
    return db_mutex_init(&sink->mutex);
}

void metrics_destroy(MetricsSink *sink) {
    if (!sink) return;
    db_mutex_destroy(&sink->mutex);
    memset(sink, 0, sizeof(*sink));
}

void metrics_record(MetricsSink *sink, const ExecutionStats *stats, int ok, CmdStatusCode status, int queue_depth) {
    if (!sink || !stats) return;
    db_mutex_lock(&sink->mutex);
    sink->values.total_requests++;
    if (!ok) sink->values.total_errors++;
    if (status == CMD_STATUS_BUSY) sink->values.total_overload++;
    sink->values.total_queue_wait_us += stats->queue_wait_us;
    sink->values.total_lock_wait_us += stats->lock_wait_us;
    sink->values.total_exec_us += stats->exec_us;
    if (stats->queue_wait_us > sink->values.max_queue_wait_us) sink->values.max_queue_wait_us = stats->queue_wait_us;
    if (stats->lock_wait_us > sink->values.max_lock_wait_us) sink->values.max_lock_wait_us = stats->lock_wait_us;
    if (stats->exec_us > sink->values.max_exec_us) sink->values.max_exec_us = stats->exec_us;
    if (queue_depth > sink->values.max_queue_depth) sink->values.max_queue_depth = queue_depth;
    db_mutex_unlock(&sink->mutex);
}

int acquire_response_slot(EngineCmdProcessorState *state, ResponseSlot **out_slot) {
    int index;
    if (out_slot) *out_slot = NULL;
    if (!state || !out_slot) return -1;
    db_mutex_lock(&state->response_mutex);
    index = state->response_free_head;
    if (index >= 0) {
        ResponseSlot *slot = &state->response_slots[index];
        state->response_free_head = slot->next_free;
        reset_response_slot(slot);
        slot->in_use = 1;
        slot->next_free = -1;
        state->current_response_slots_in_use++;
        if (state->current_response_slots_in_use > state->peak_response_slots_in_use) {
            state->peak_response_slots_in_use = state->current_response_slots_in_use;
        }
        *out_slot = slot;
        db_mutex_unlock(&state->response_mutex);
        return 0;
    }
    db_mutex_unlock(&state->response_mutex);
    return -1;
}

RequestSlot *request_slot_from_request(EngineCmdProcessorState *state, CmdRequest *request) {
    ptrdiff_t index;
    if (!state || !request || !state->request_slots) return NULL;
    if (request < &state->request_slots[0].request ||
        request > &state->request_slots[state->request_count - 1].request) {
        return NULL;
    }
    index = ((RequestSlot *)request) - state->request_slots;
    if (index < 0 || (size_t)index >= state->request_count) return NULL;
    return &state->request_slots[index];
}

ResponseSlot *response_slot_from_response(EngineCmdProcessorState *state, CmdResponse *response) {
    ptrdiff_t index;
    if (!state || !response || !state->response_slots) return NULL;
    if (response < &state->response_slots[0].response ||
        response > &state->response_slots[state->response_count - 1].response) {
        return NULL;
    }
    index = ((ResponseSlot *)response) - state->response_slots;
    if (index < 0 || (size_t)index >= state->response_count) return NULL;
    return &state->response_slots[index];
}

static void init_response_common(ResponseSlot *slot, const char *request_id, CmdStatusCode status, int ok) {
    memset(&slot->response, 0, sizeof(slot->response));
    copy_fixed(slot->response.request_id, sizeof(slot->response.request_id), request_id);
    slot->response.status = status;
    slot->response.ok = ok;
    slot->response.body_format = CMD_BODY_NONE;
}

void set_response_error(ResponseSlot *slot, const char *request_id, CmdStatusCode status, const char *message, size_t error_capacity) {
    init_response_common(slot, request_id, status, 0);
    if (message) {
        copy_fixed(slot->message_buffer, error_capacity + 1, message);
        slot->response.error_message = slot->message_buffer;
    }
}

void set_response_body(ResponseSlot *slot, const char *request_id, CmdBodyFormat body_format,
                              const char *body, size_t body_len, size_t body_capacity) {
    init_response_common(slot, request_id, CMD_STATUS_OK, 1);
    if (body && body_len <= body_capacity) {
        memmove(slot->message_buffer, body, body_len);
        slot->message_buffer[body_len] = '\0';
        slot->response.body = slot->message_buffer;
        slot->response.body_len = body_len;
        slot->response.body_format = body_format;
    }
}

int engine_execute_sql(EngineCmdProcessorState *state, CmdRequest *request, ResponseSlot *response_slot) {
    Statement stmt;
    ExecutorResult result;

    if (request->type == CMD_REQUEST_PING) {
        set_response_body(response_slot, request->request_id, CMD_BODY_TEXT, "pong", 4, state->response_body_capacity);
        return 1;
    }
    if (request->type != CMD_REQUEST_SQL || !request->sql || request->sql[0] == '\0') {
        set_invalid_sql_request_error(state, request, response_slot);
        return 0;
    }

    if (!parse_statement_or_respond(state, request, response_slot, &stmt)) {
        return 0;
    }
    executor_result_init(&result);
    if (!execute_statement_or_respond(state,
                                      request,
                                      response_slot,
                                      &stmt,
                                      &result)) {
        executor_result_free(&result);
        return 0;
    }

    set_statement_success_response(state,
                                   request,
                                   response_slot,
                                   &stmt,
                                   &result);
    executor_result_free(&result);
    return 1;
}

int execute_planned_request(EngineCmdProcessorState *state,
                            CmdRequest *request,
                            ResponseSlot *response_slot,
                            const LockPlan *lock_plan,
                            ExecutionStats *stats) {
    LockHandle handles[ENGINE_MAX_TABLES_PER_PLAN];
    uint64_t exec_start_us;

    if (!state || !request || !response_slot || !lock_plan || !stats) return 0;

    memset(handles, 0, sizeof(handles));
    if (!lock_manager_acquire(&state->lock_manager, lock_plan, handles, &stats->lock_wait_us)) {
        set_response_error(response_slot,
                           request->request_id,
                           CMD_STATUS_INTERNAL_ERROR,
                           "lock acquisition failed",
                           state->response_body_capacity);
        return 0;
    }

    exec_start_us = monotonic_us();
    db_mutex_lock(&state->state_mutex);
    state->current_concurrent_executions++;
    if (state->current_concurrent_executions > state->max_concurrent_executions) {
        state->max_concurrent_executions = state->current_concurrent_executions;
    }
    db_mutex_unlock(&state->state_mutex);
    engine_execute_sql(state, request, response_slot);
    stats->exec_us = monotonic_us() - exec_start_us;
    db_mutex_lock(&state->state_mutex);
    if (state->current_concurrent_executions > 0) state->current_concurrent_executions--;
    db_mutex_unlock(&state->state_mutex);
    lock_manager_release(&state->lock_manager, lock_plan, handles);
    return 1;
}

static void job_complete(EngineCmdProcessorState *state, CmdJob *job) {
    CmdProcessorResponseCallback callback;
    void *user_data;
    CmdProcessor *processor;
    CmdRequest *request;
    CmdResponse *response;

    if (!state || !job) return;
    callback = job->callback;
    user_data = job->user_data;
    processor = job->processor;
    request = job->request;
    response = job->response_slot ? &job->response_slot->response : NULL;

    if (callback) {
        callback(processor, request, response, user_data);
    }

    release_job(state, job);
}

db_thread_return_t DB_THREAD_CALL worker_main(void *arg_ptr) {
    WorkerArgs *arg = (WorkerArgs *)arg_ptr;
    EngineCmdProcessorState *state = arg->state;
    WorkQueue *queue = &state->queues[arg->shard_id];

    for (;;) {
        CmdJob *job = (CmdJob *)work_queue_pop(queue);

        if (!job) break;

        job->stats.queue_wait_us = monotonic_us() - job->enqueue_us;
        execute_planned_request(state,
                                job->request,
                                job->response_slot,
                                &job->lock_plan,
                                &job->stats);
        record_completed_job_metrics(state, job);
        job_complete(state, job);
    }

    free(arg);
#if defined(_WIN32)
    return 0;
#else
    return NULL;
#endif
}

