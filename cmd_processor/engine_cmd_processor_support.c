#include "engine_cmd_processor_internal.h"

uint64_t monotonic_us(void) {
#if defined(_WIN32)
    static LARGE_INTEGER frequency;
    LARGE_INTEGER counter;
    if (frequency.QuadPart == 0) QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&counter);
    return (uint64_t)((counter.QuadPart * 1000000ULL) / frequency.QuadPart);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
#endif
}

unsigned long hash_text(const char *text) {
    unsigned long hash = 5381UL;
    while (text && *text) {
        hash = ((hash << 5) + hash) + (unsigned char)*text;
        text++;
    }
    return hash;
}

void copy_fixed(char *dst, size_t dst_size, const char *src) {
    size_t len = 0;
    if (!dst || dst_size == 0) return;
    if (src) len = strlen(src);
    if (len >= dst_size) len = dst_size - 1;
    if (len > 0) memcpy(dst, src, len);
    dst[len] = '\0';
}

void reset_request_slot(RequestSlot *slot) {
    if (!slot) return;
    memset(&slot->request, 0, sizeof(slot->request));
    slot->request.type = (CmdRequestType)-1;
    slot->request.sql = slot->sql_buffer;
    if (slot->sql_buffer) slot->sql_buffer[0] = '\0';
    slot->in_use = 0;
}

void reset_response_slot(ResponseSlot *slot) {
    if (!slot) return;
    memset(&slot->response, 0, sizeof(slot->response));
    slot->response.status = CMD_STATUS_INTERNAL_ERROR;
    slot->response.body_format = CMD_BODY_NONE;
    if (slot->message_buffer) slot->message_buffer[0] = '\0';
    slot->in_use = 0;
}

EngineCmdProcessorState *state_from_context(CmdProcessorContext *context) {
    if (!context) return NULL;
    return (EngineCmdProcessorState *)context->shared_state;
}

EngineCmdProcessorState *state_from_processor(CmdProcessor *processor) {
    if (!processor || !processor->context) return NULL;
    return state_from_context(processor->context);
}

int memory_pool_init(MemoryPool *pool, size_t object_size) {
    if (!pool || object_size < sizeof(MemoryPoolNode)) return 0;
    memset(pool, 0, sizeof(*pool));
    pool->object_size = object_size;
    return db_mutex_init(&pool->mutex);
}

int memory_pool_prewarm(MemoryPool *pool, int count) {
    int i;
    if (!pool || count <= 0) return 1;
    db_mutex_lock(&pool->mutex);
    for (i = 0; i < count; i++) {
        MemoryPoolNode *node = (MemoryPoolNode *)calloc(1, pool->object_size);
        if (!node) {
            db_mutex_unlock(&pool->mutex);
            return 0;
        }
        node->next = pool->free_list;
        pool->free_list = node;
        pool->total_allocated++;
    }
    db_mutex_unlock(&pool->mutex);
    return 1;
}

void memory_pool_destroy(MemoryPool *pool) {
    MemoryPoolNode *node;
    MemoryPoolNode *next;
    if (!pool) return;
    node = pool->free_list;
    while (node) {
        next = node->next;
        free(node);
        node = next;
    }
    db_mutex_destroy(&pool->mutex);
    memset(pool, 0, sizeof(*pool));
}

static void *memory_pool_acquire(MemoryPool *pool) {
    MemoryPoolNode *node = NULL;
    void *fresh = NULL;
    if (!pool) return NULL;
    db_mutex_lock(&pool->mutex);
    if (pool->free_list) {
        node = pool->free_list;
        pool->free_list = node->next;
        pool->current_in_use++;
        if (pool->current_in_use > pool->peak_in_use) pool->peak_in_use = pool->current_in_use;
        db_mutex_unlock(&pool->mutex);
        return node;
    }
    db_mutex_unlock(&pool->mutex);
    fresh = calloc(1, pool->object_size);
    if (!fresh) return NULL;
    db_mutex_lock(&pool->mutex);
    pool->total_allocated++;
    pool->current_in_use++;
    if (pool->current_in_use > pool->peak_in_use) pool->peak_in_use = pool->current_in_use;
    db_mutex_unlock(&pool->mutex);
    return fresh;
}

static void memory_pool_release(MemoryPool *pool, void *ptr) {
    MemoryPoolNode *node;
    if (!pool || !ptr) return;
    node = (MemoryPoolNode *)ptr;
    db_mutex_lock(&pool->mutex);
    if (pool->current_in_use > 0) pool->current_in_use--;
    node->next = pool->free_list;
    pool->free_list = node;
    db_mutex_unlock(&pool->mutex);
}

CmdJob *acquire_job(EngineCmdProcessorState *state) {
    CmdJob *job;
    if (!state) return NULL;
    job = (CmdJob *)memory_pool_acquire(&state->job_pool);
    if (!job) return NULL;
    job->processor = NULL;
    job->request = NULL;
    job->response_slot = NULL;
    job->callback = NULL;
    job->user_data = NULL;
    memset(&job->route_plan, 0, sizeof(job->route_plan));
    memset(&job->lock_plan, 0, sizeof(job->lock_plan));
    memset(&job->stats, 0, sizeof(job->stats));
    job->enqueue_us = 0;
    job->enqueue_depth = 0;
    return job;
}

void release_job(EngineCmdProcessorState *state, CmdJob *job) {
    if (!state || !job) return;
    memory_pool_release(&state->job_pool, job);
}

void destroy_job_pool(MemoryPool *pool) {
    if (!pool) return;
    memory_pool_destroy(pool);
}

int work_queue_init(WorkQueue *queue, int capacity) {
    if (!queue || capacity <= 0) return 0;
    memset(queue, 0, sizeof(*queue));
    queue->items = (void **)calloc((size_t)capacity, sizeof(void *));
    if (!queue->items) return 0;
    queue->capacity = capacity;
    if (!db_mutex_init(&queue->mutex) || !db_cond_init(&queue->not_empty) || !db_cond_init(&queue->not_full)) {
        free(queue->items);
        queue->items = NULL;
        return 0;
    }
    return 1;
}

void work_queue_destroy(WorkQueue *queue) {
    if (!queue) return;
    db_cond_destroy(&queue->not_empty);
    db_cond_destroy(&queue->not_full);
    db_mutex_destroy(&queue->mutex);
    free(queue->items);
    memset(queue, 0, sizeof(*queue));
}

int work_queue_push(WorkQueue *queue, void *item, int *depth_out) {
    if (depth_out) *depth_out = 0;
    if (!queue) return 0;
    db_mutex_lock(&queue->mutex);
    while (!queue->shutdown && queue->count >= queue->capacity) {
        db_cond_wait(&queue->not_full, &queue->mutex);
    }
    if (queue->shutdown) {
        db_mutex_unlock(&queue->mutex);
        return 0;
    }
    queue->items[queue->tail] = item;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    if (depth_out) *depth_out = queue->count;
    db_cond_signal(&queue->not_empty);
    db_mutex_unlock(&queue->mutex);
    return 1;
}

void *work_queue_try_pop(WorkQueue *queue) {
    void *item = NULL;

    if (!queue) return NULL;
    db_mutex_lock(&queue->mutex);
    if (queue->count > 0) {
        item = queue->items[queue->head];
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
        db_cond_signal(&queue->not_full);
    }
    db_mutex_unlock(&queue->mutex);
    return item;
}

void *work_queue_pop_timed(WorkQueue *queue, uint64_t timeout_ms) {
    void *item = NULL;

    if (!queue) return NULL;
    db_mutex_lock(&queue->mutex);
    while (!queue->shutdown && queue->count == 0) {
        if (!db_cond_timedwait(&queue->not_empty, &queue->mutex, timeout_ms)) {
            db_mutex_unlock(&queue->mutex);
            return NULL;
        }
    }
    if (queue->count > 0) {
        item = queue->items[queue->head];
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
        db_cond_signal(&queue->not_full);
    }
    db_mutex_unlock(&queue->mutex);
    return item;
}

void *work_queue_pop(WorkQueue *queue) {
    void *item;
    if (!queue) return NULL;
    db_mutex_lock(&queue->mutex);
    while (!queue->shutdown && queue->count == 0) {
        db_cond_wait(&queue->not_empty, &queue->mutex);
    }
    if (queue->count == 0) {
        db_mutex_unlock(&queue->mutex);
        return NULL;
    }
    item = queue->items[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    db_cond_signal(&queue->not_full);
    db_mutex_unlock(&queue->mutex);
    return item;
}

void work_queue_shutdown(WorkQueue *queue) {
    if (!queue) return;
    db_mutex_lock(&queue->mutex);
    queue->shutdown = 1;
    db_cond_broadcast(&queue->not_empty);
    db_cond_broadcast(&queue->not_full);
    db_mutex_unlock(&queue->mutex);
}

