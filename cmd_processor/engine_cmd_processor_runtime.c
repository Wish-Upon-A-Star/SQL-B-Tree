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
                                        int *matched_rows,
                                        int *affected_rows,
                                        long *generated_id) {
    if (executor_execute_statement(stmt, matched_rows, affected_rows, generated_id)) return 1;

    set_response_error(response_slot,
                       request->request_id,
                       CMD_STATUS_PROCESSING_ERROR,
                       "execution failed",
                       state->response_body_capacity);
    return 0;
}

static void set_statement_success_response(EngineCmdProcessorState *state,
                                           CmdRequest *request,
                                           ResponseSlot *response_slot,
                                           const Statement *stmt,
                                           int matched_rows,
                                           int affected_rows,
                                           long generated_id) {
    char body[256];

    if (stmt->type == STMT_SELECT) {
        snprintf(body, sizeof(body), "SELECT matched_rows=%d", matched_rows);
    } else if (stmt->type == STMT_INSERT) {
        snprintf(body, sizeof(body), "INSERT affected_rows=%d id=%ld", affected_rows, generated_id);
    } else {
        snprintf(body,
                 sizeof(body),
                 "%s affected_rows=%d",
                 stmt->type == STMT_UPDATE ? "UPDATE" : "DELETE",
                 affected_rows);
    }

    set_response_body(response_slot,
                      request->request_id,
                      CMD_BODY_TEXT,
                      body,
                      strlen(body),
                      state->response_body_capacity);
    response_slot->response.row_count = matched_rows;
    response_slot->response.affected_count = affected_rows;
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
        memcpy(slot->message_buffer, body, body_len);
        slot->message_buffer[body_len] = '\0';
        slot->response.body = slot->message_buffer;
        slot->response.body_len = body_len;
        slot->response.body_format = body_format;
    }
}

int engine_execute_sql(EngineCmdProcessorState *state, CmdRequest *request, ResponseSlot *response_slot) {
    Statement stmt;
    int matched_rows = 0;
    int affected_rows = 0;
    long generated_id = 0;

    if (request->type == CMD_REQUEST_PING) {
        set_response_body(response_slot, request->request_id, CMD_BODY_TEXT, "pong", 4, state->response_body_capacity);
        return 1;
    }
    if (request->type != CMD_REQUEST_SQL || !request->sql || request->sql[0] == '\0') {
        set_invalid_sql_request_error(state, request, response_slot);
        return 0;
    }

    db_mutex_lock(&state->engine_mutex);
    if (!parse_statement_or_respond(state, request, response_slot, &stmt)) {
        db_mutex_unlock(&state->engine_mutex);
        return 0;
    }
    if (!execute_statement_or_respond(state,
                                      request,
                                      response_slot,
                                      &stmt,
                                      &matched_rows,
                                      &affected_rows,
                                      &generated_id)) {
        db_mutex_unlock(&state->engine_mutex);
        return 0;
    }
    db_mutex_unlock(&state->engine_mutex);

    set_statement_success_response(state,
                                   request,
                                   response_slot,
                                   &stmt,
                                   matched_rows,
                                   affected_rows,
                                   generated_id);
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
    engine_execute_sql(state, request, response_slot);
    stats->exec_us = monotonic_us() - exec_start_us;
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

