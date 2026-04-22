#include "engine_cmd_processor_internal.h"

static void publish_error_response(CmdProcessor *processor,
                                   CmdRequest *request,
                                   ResponseSlot *response_slot,
                                   CmdProcessorResponseCallback callback,
                                   void *user_data,
                                   CmdStatusCode status,
                                   const char *message,
                                   size_t response_body_capacity) {
    set_response_error(response_slot,
                       request->request_id,
                       status,
                       message,
                       response_body_capacity);
    callback(processor, request, &response_slot->response, user_data);
}

static void push_request_slot_free_list(EngineCmdProcessorState *state, RequestSlot *slot) {
    if (state->current_request_slots_in_use > 0) state->current_request_slots_in_use--;
    slot->next_free = state->request_free_head;
    state->request_free_head = (int)(slot - state->request_slots);
}

static void push_response_slot_free_list(EngineCmdProcessorState *state, ResponseSlot *slot) {
    if (state->current_response_slots_in_use > 0) state->current_response_slots_in_use--;
    slot->next_free = state->response_free_head;
    state->response_free_head = (int)(slot - state->response_slots);
}

static int enqueue_planned_job(EngineCmdProcessorState *state,
                               CmdProcessor *processor,
                               CmdRequest *request,
                               ResponseSlot *response_slot,
                               CmdProcessorResponseCallback callback,
                               void *user_data,
                               const RoutePlan *route_plan,
                               const LockPlan *lock_plan);

static int processor_acquire_request(CmdProcessorContext *context, CmdRequest **out_request) {
    EngineCmdProcessorState *state = state_from_context(context);
    int index;
    if (out_request) *out_request = NULL;
    if (!state || !out_request) return -1;
    db_mutex_lock(&state->request_mutex);
    index = state->request_free_head;
    if (index >= 0) {
        RequestSlot *slot = &state->request_slots[index];
        state->request_free_head = slot->next_free;
        reset_request_slot(slot);
        slot->in_use = 1;
        slot->next_free = -1;
        state->current_request_slots_in_use++;
        if (state->current_request_slots_in_use > state->peak_request_slots_in_use) {
            state->peak_request_slots_in_use = state->current_request_slots_in_use;
        }
        *out_request = &slot->request;
        db_mutex_unlock(&state->request_mutex);
        return 0;
    }
    db_mutex_unlock(&state->request_mutex);
    return -1;
}

static void deliver_inline_response(EngineCmdProcessorState *state,
                                    CmdProcessor *processor,
                                    CmdRequest *request,
                                    ResponseSlot *response_slot,
                                    CmdProcessorResponseCallback callback,
                                    void *user_data,
                                    const ExecutionStats *stats) {
    metrics_record(&state->metrics,
                   stats,
                   response_slot->response.ok,
                   response_slot->response.status,
                   0);
    callback(processor, request, &response_slot->response, user_data);
}

static int submit_ping_inline(EngineCmdProcessorState *state,
                              CmdProcessor *processor,
                              CmdRequest *request,
                              ResponseSlot *response_slot,
                              CmdProcessorResponseCallback callback,
                              void *user_data,
                              uint64_t start_us) {
    ExecutionStats stats;

    memset(&stats, 0, sizeof(stats));
    set_response_body(response_slot,
                      request->request_id,
                      CMD_BODY_TEXT,
                      "pong",
                      4,
                      state->response_body_capacity);
    stats.exec_us = monotonic_us() - start_us;
    stats.total_us = stats.exec_us;
    deliver_inline_response(state, processor, request, response_slot, callback, user_data, &stats);
    return 0;
}

static int plan_request_or_respond(EngineCmdProcessorState *state,
                                   CmdProcessor *processor,
                                   CmdRequest *request,
                                   ResponseSlot *response_slot,
                                   CmdProcessorResponseCallback callback,
                                   void *user_data,
                                   RoutePlan *route_plan,
                                   LockPlan *lock_plan) {
    if (build_request_plan(state, request, route_plan, lock_plan)) return 1;

    publish_error_response(processor,
                           request,
                           response_slot,
                           callback,
                           user_data,
                           CMD_STATUS_BAD_REQUEST,
                           "request planning failed",
                           state->response_body_capacity);
    return 0;
}

static int enqueue_planned_job(EngineCmdProcessorState *state,
                               CmdProcessor *processor,
                               CmdRequest *request,
                               ResponseSlot *response_slot,
                               CmdProcessorResponseCallback callback,
                               void *user_data,
                               const RoutePlan *route_plan,
                               const LockPlan *lock_plan) {
    CmdJob *job;
    int queue_depth = 0;

    job = acquire_job(state);
    if (!job) {
        publish_error_response(processor,
                               request,
                               response_slot,
                               callback,
                               user_data,
                               CMD_STATUS_INTERNAL_ERROR,
                               "job allocation failed",
                               state->response_body_capacity);
        return 0;
    }

    job->processor = processor;
    job->request = request;
    job->response_slot = response_slot;
    job->callback = callback;
    job->user_data = user_data;
    job->route_plan = *route_plan;
    job->lock_plan = *lock_plan;
    job->enqueue_us = monotonic_us();

    if (!state->running || !work_queue_push(&state->queues[route_plan->target_shard], job, &queue_depth)) {
        publish_error_response(processor,
                               request,
                               response_slot,
                               callback,
                               user_data,
                               CMD_STATUS_BUSY,
                               "processor is not accepting requests",
                               state->response_body_capacity);
        release_job(state, job);
        return 0;
    }

    job->enqueue_depth = queue_depth;
    return 0;
}

static int processor_submit(CmdProcessor *processor,
                            CmdProcessorContext *context,
                            CmdRequest *request,
                            CmdProcessorResponseCallback callback,
                            void *user_data) {
    EngineCmdProcessorState *state = state_from_context(context);
                            ResponseSlot *response_slot = NULL;
    RequestSlot *request_slot;
    RoutePlan route_plan;
    LockPlan lock_plan;
    uint64_t start_us;

    if (!state || !processor || !request || !callback) return -1;

    start_us = monotonic_us();
    request_slot = request_slot_from_request(state, request);
    if (!request_slot || !request_slot->in_use) return -1;
    if (acquire_response_slot(state, &response_slot) != 0) return -1;

    if (request->type == CMD_REQUEST_PING) {
        return submit_ping_inline(state, processor, request, response_slot, callback, user_data, start_us);
    }

    if (!plan_request_or_respond(state,
                                 processor,
                                 request,
                                 response_slot,
                                 callback,
                                 user_data,
                                 &route_plan,
                                 &lock_plan)) {
        return 0;
    }

    return enqueue_planned_job(state,
                               processor,
                               request,
                               response_slot,
                               callback,
                               user_data,
                               &route_plan,
                               &lock_plan);
}

static int processor_make_error_response(CmdProcessorContext *context, const char *request_id, CmdStatusCode status, const char *error_message, CmdResponse **out_response) {
    EngineCmdProcessorState *state = state_from_context(context);
    ResponseSlot *slot = NULL;
    if (out_response) *out_response = NULL;
    if (!state || !out_response) return -1;
    if (acquire_response_slot(state, &slot) != 0) return -1;
    set_response_error(slot, request_id, status, error_message, state->response_body_capacity);
    *out_response = &slot->response;
    return 0;
}

static void processor_release_request(CmdProcessorContext *context, CmdRequest *request) {
    EngineCmdProcessorState *state = state_from_context(context);
    RequestSlot *slot;
    if (!state || !request) return;
    slot = request_slot_from_request(state, request);
    if (!slot) return;
    db_mutex_lock(&state->request_mutex);
    reset_request_slot(slot);
    push_request_slot_free_list(state, slot);
    db_mutex_unlock(&state->request_mutex);
}

static void processor_release_response(CmdProcessorContext *context, CmdResponse *response) {
    EngineCmdProcessorState *state = state_from_context(context);
    ResponseSlot *slot;
    if (!state || !response) return;
    slot = response_slot_from_response(state, response);
    if (!slot) return;
    db_mutex_lock(&state->response_mutex);
    reset_response_slot(slot);
    push_response_slot_free_list(state, slot);
    db_mutex_unlock(&state->response_mutex);
}

static void processor_shutdown(CmdProcessorContext *context) {
    EngineCmdProcessorState *state = state_from_context(context);
    int i;
    if (!state) return;

    db_mutex_lock(&state->state_mutex);
    state->running = 0;
    db_mutex_unlock(&state->state_mutex);

    if (state->queues) {
        for (i = 0; i < state->options.shard_count; i++) work_queue_shutdown(&state->queues[i]);
    }
    for (i = 0; i < state->thread_count; i++) db_thread_join(state->threads[i]);

    close_all_tables();

    if (state->queues) {
        for (i = 0; i < state->options.shard_count; i++) work_queue_destroy(&state->queues[i]);
    }
    if (state->request_slots) {
        for (i = 0; i < (int)state->request_count; i++) free(state->request_slots[i].sql_buffer);
    }
    if (state->response_slots) {
        for (i = 0; i < (int)state->response_count; i++) {
            free(state->response_slots[i].message_buffer);
        }
    }

    free(state->queues);
    free(state->threads);
    free(state->request_slots);
    free(state->response_slots);
    destroy_job_pool(&state->job_pool);
    planner_cache_destroy(&state->planner_cache);
    lock_manager_destroy(&state->lock_manager);
    metrics_destroy(&state->metrics);
    db_mutex_destroy(&state->response_mutex);
    db_mutex_destroy(&state->request_mutex);
    db_mutex_destroy(&state->state_mutex);
    executor_runtime_shutdown();
    free(state);
}

static void apply_default_options(EngineCmdProcessorOptions *options) {
    if (options->worker_count <= 0) options->worker_count = ENGINE_DEFAULT_WORKERS;
    if (options->shard_count <= 0) options->shard_count = ENGINE_DEFAULT_SHARDS;
    if (options->queue_capacity_per_shard <= 0) options->queue_capacity_per_shard = ENGINE_DEFAULT_QUEUE_CAPACITY;
    if (options->planner_cache_capacity <= 0) options->planner_cache_capacity = ENGINE_DEFAULT_PLANNER_CACHE_CAPACITY;
}

static void apply_default_context(EngineCmdProcessorState *state, const CmdProcessorContext *base_context) {
    const char *name = ENGINE_DEFAULT_NAME;
    size_t max_sql_len = ENGINE_DEFAULT_MAX_SQL_LEN;
    size_t request_count = 0;
    size_t body_capacity = ENGINE_DEFAULT_BODY_CAPACITY;

    if (base_context) {
        if (base_context->name) name = base_context->name;
        if (base_context->max_sql_len > 0) max_sql_len = base_context->max_sql_len;
        if (base_context->request_buffer_count > 0) request_count = base_context->request_buffer_count;
        if (base_context->response_body_capacity > 0) body_capacity = base_context->response_body_capacity;
    }
    if (request_count == 0) {
        request_count = (size_t)(state->options.worker_count + state->options.shard_count * state->options.queue_capacity_per_shard + 8);
    }

    memset(&state->context, 0, sizeof(state->context));
    state->context.name = name;
    state->context.max_sql_len = max_sql_len;
    state->context.request_buffer_count = request_count;
    state->context.response_body_capacity = body_capacity;
    state->context.shared_state = state;
    state->max_sql_len = max_sql_len;
    state->request_count = request_count;
    state->response_count = request_count;
    state->response_body_capacity = body_capacity;
}

static int allocate_slots(EngineCmdProcessorState *state) {
    size_t i;
    state->request_free_head = -1;
    state->response_free_head = -1;
    state->request_slots = (RequestSlot *)calloc(state->request_count, sizeof(RequestSlot));
    state->response_slots = (ResponseSlot *)calloc(state->response_count, sizeof(ResponseSlot));
    if (!state->request_slots || !state->response_slots) return 0;
    state->reserved_bytes += (unsigned long long)(state->request_count * sizeof(RequestSlot));
    state->reserved_bytes += (unsigned long long)(state->response_count * sizeof(ResponseSlot));
    for (i = 0; i < state->request_count; i++) {
        state->request_slots[i].sql_buffer = (char *)calloc(state->max_sql_len + 1, 1);
        if (!state->request_slots[i].sql_buffer) return 0;
        state->reserved_bytes += (unsigned long long)(state->max_sql_len + 1);
        reset_request_slot(&state->request_slots[i]);
        state->request_slots[i].next_free = (int)i + 1;
    }
    if (state->request_count > 0) state->request_slots[state->request_count - 1].next_free = -1;
    state->request_free_head = state->request_count > 0 ? 0 : -1;
    for (i = 0; i < state->response_count; i++) {
        state->response_slots[i].message_buffer = (char *)calloc(state->response_body_capacity + 1, 1);
        if (!state->response_slots[i].message_buffer) return 0;
        state->reserved_bytes += (unsigned long long)(state->response_body_capacity + 1);
        reset_response_slot(&state->response_slots[i]);
        state->response_slots[i].next_free = (int)i + 1;
    }
    if (state->response_count > 0) state->response_slots[state->response_count - 1].next_free = -1;
    state->response_free_head = state->response_count > 0 ? 0 : -1;
    return 1;
}

int engine_cmd_processor_create(const CmdProcessorContext *base_context,
                                const EngineCmdProcessorOptions *options,
                                CmdProcessor **out_processor) {
    EngineCmdProcessorState *state;
    EngineCmdProcessorOptions local_options;
    int i;
    int state_mutex_ready = 0;
    int request_mutex_ready = 0;
    int response_mutex_ready = 0;
    int job_pool_ready = 0;
    int planner_cache_ready = 0;
    int lock_manager_ready = 0;
    int metrics_ready = 0;
    int executor_ready = 0;
    int queue_count = 0;

    if (out_processor) *out_processor = NULL;
    if (!out_processor) return -1;

    memset(&local_options, 0, sizeof(local_options));
    if (options) local_options = *options;
    apply_default_options(&local_options);

    state = (EngineCmdProcessorState *)calloc(1, sizeof(*state));
    if (!state) return -1;
    state->options = local_options;
    state->reserved_bytes = (unsigned long long)sizeof(*state);

    state_mutex_ready = db_mutex_init(&state->state_mutex);
    request_mutex_ready = state_mutex_ready ? db_mutex_init(&state->request_mutex) : 0;
    response_mutex_ready = request_mutex_ready ? db_mutex_init(&state->response_mutex) : 0;
    job_pool_ready = response_mutex_ready ? memory_pool_init(&state->job_pool, sizeof(CmdJob)) : 0;
    planner_cache_ready = job_pool_ready ? planner_cache_init(&state->planner_cache, state->options.planner_cache_capacity) : 0;
    lock_manager_ready = planner_cache_ready ? lock_manager_init(&state->lock_manager) : 0;
    metrics_ready = lock_manager_ready ? metrics_init(&state->metrics) : 0;
    executor_ready = metrics_ready ? executor_runtime_init() : 0;

    if (!metrics_ready || !executor_ready) {
        if (metrics_ready) metrics_destroy(&state->metrics);
        if (lock_manager_ready) lock_manager_destroy(&state->lock_manager);
        if (planner_cache_ready) planner_cache_destroy(&state->planner_cache);
        if (job_pool_ready) memory_pool_destroy(&state->job_pool);
        if (response_mutex_ready) db_mutex_destroy(&state->response_mutex);
        if (request_mutex_ready) db_mutex_destroy(&state->request_mutex);
        if (state_mutex_ready) db_mutex_destroy(&state->state_mutex);
        free(state);
        return -1;
    }

    apply_default_context(state, base_context);
    if (!memory_pool_prewarm(&state->job_pool,
                             state->options.worker_count + state->options.shard_count * state->options.queue_capacity_per_shard)) {
        goto fail;
    }
    state->reserved_bytes += state->job_pool.total_allocated * (unsigned long long)sizeof(CmdJob);
    if (!allocate_slots(state)) {
        goto fail;
    }

    state->queues = (WorkQueue *)calloc((size_t)state->options.shard_count, sizeof(WorkQueue));
    state->threads = (db_thread_t *)calloc((size_t)state->options.worker_count, sizeof(db_thread_t));
    if (!state->queues || !state->threads) {
        goto fail;
    }
    state->reserved_bytes += (unsigned long long)(state->options.shard_count * (int)sizeof(WorkQueue));
    state->reserved_bytes += (unsigned long long)(state->options.worker_count * (int)sizeof(db_thread_t));
    for (i = 0; i < state->options.shard_count; i++) {
        if (!work_queue_init(&state->queues[i], state->options.queue_capacity_per_shard)) {
            goto fail;
        }
        state->reserved_bytes += (unsigned long long)(state->options.queue_capacity_per_shard * (int)sizeof(void *));
        queue_count++;
    }
    state->reserved_bytes += (unsigned long long)(state->options.planner_cache_capacity * (int)sizeof(PlannerCacheEntry));

    state->processor.context = &state->context;
    state->processor.acquire_request = processor_acquire_request;
    state->processor.submit = processor_submit;
    state->processor.make_error_response = processor_make_error_response;
    state->processor.release_request = processor_release_request;
    state->processor.release_response = processor_release_response;
    state->processor.shutdown = processor_shutdown;
    state->running = 1;

    for (i = 0; i < state->options.worker_count; i++) {
        WorkerArgs *args = (WorkerArgs *)calloc(1, sizeof(WorkerArgs));
        if (!args) {
            goto fail;
        }
        args->state = state;
        args->worker_id = i;
        args->shard_id = i % state->options.shard_count;
        if (!db_thread_create(&state->threads[i], worker_main, args)) {
            free(args);
            goto fail;
        }
        state->thread_count++;
    }

    *out_processor = &state->processor;
    return 0;

fail:
    state->running = 0;
    for (i = 0; i < queue_count; i++) work_queue_shutdown(&state->queues[i]);
    for (i = 0; i < state->thread_count; i++) db_thread_join(state->threads[i]);
    for (i = 0; i < queue_count; i++) work_queue_destroy(&state->queues[i]);
    if (state->request_slots) {
        for (i = 0; i < (int)state->request_count; i++) free(state->request_slots[i].sql_buffer);
    }
    if (state->response_slots) {
        for (i = 0; i < (int)state->response_count; i++) {
            free(state->response_slots[i].message_buffer);
        }
    }
    free(state->queues);
    free(state->threads);
    free(state->request_slots);
    free(state->response_slots);
    metrics_destroy(&state->metrics);
    lock_manager_destroy(&state->lock_manager);
    planner_cache_destroy(&state->planner_cache);
    destroy_job_pool(&state->job_pool);
    db_mutex_destroy(&state->response_mutex);
    db_mutex_destroy(&state->request_mutex);
    db_mutex_destroy(&state->state_mutex);
    executor_runtime_shutdown();
    free(state);
    return -1;
}

int engine_cmd_processor_snapshot_stats(CmdProcessor *processor,
                                        EngineCmdProcessorStats *out_stats) {
    EngineCmdProcessorState *state = state_from_processor(processor);
    if (!state || !out_stats) return -1;
    db_mutex_lock(&state->metrics.mutex);
    *out_stats = state->metrics.values;
    out_stats->reserved_bytes = state->reserved_bytes;
    db_mutex_unlock(&state->metrics.mutex);
    db_mutex_lock(&state->job_pool.mutex);
    out_stats->peak_jobs_in_use = state->job_pool.peak_in_use;
    out_stats->peak_jobs_allocated = state->job_pool.total_allocated;
    db_mutex_unlock(&state->job_pool.mutex);
    db_mutex_lock(&state->request_mutex);
    out_stats->peak_request_slots_in_use = state->peak_request_slots_in_use;
    db_mutex_unlock(&state->request_mutex);
    db_mutex_lock(&state->response_mutex);
    out_stats->peak_response_slots_in_use = state->peak_response_slots_in_use;
    db_mutex_unlock(&state->response_mutex);
    db_mutex_lock(&state->state_mutex);
    out_stats->max_concurrent_executions = state->max_concurrent_executions;
    db_mutex_unlock(&state->state_mutex);
    return 0;
}
