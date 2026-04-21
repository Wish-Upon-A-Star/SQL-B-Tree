#ifndef ENGINE_CMD_PROCESSOR_INTERNAL_H
#define ENGINE_CMD_PROCESSOR_INTERNAL_H

#include "engine_cmd_processor.h"

#include <ctype.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../executor.h"
#include "../parser.h"
#include "../platform_threads.h"

#define ENGINE_DEFAULT_NAME "engine_cmd_processor"
#define ENGINE_DEFAULT_MAX_SQL_LEN 4096u
#define ENGINE_DEFAULT_BODY_CAPACITY 4096u
#define ENGINE_DEFAULT_WORKERS 2
#define ENGINE_DEFAULT_SHARDS 1
#define ENGINE_DEFAULT_QUEUE_CAPACITY 64
#define ENGINE_DEFAULT_PLANNER_CACHE_CAPACITY 128
#define ENGINE_MAX_TABLES_PER_PLAN 4
#define ENGINE_LOCK_BUCKETS 32

typedef struct CmdJob CmdJob;

typedef enum {
    ENGINE_REQUEST_CLASS_READ = 1,
    ENGINE_REQUEST_CLASS_WRITE = 2,
    ENGINE_REQUEST_CLASS_CONTROL = 3
} EngineRequestClass;

typedef enum {
    ENGINE_LOCK_READ = 0,
    ENGINE_LOCK_WRITE = 1
} EngineLockMode;

typedef struct {
    char name[256];
    EngineLockMode mode;
} EngineLockTarget;

typedef struct {
    EngineRequestClass request_class;
    uint32_t route_key;
    int target_shard;
    int target_table_count;
    char target_tables[ENGINE_MAX_TABLES_PER_PLAN][256];
} RoutePlan;

typedef struct {
    int lock_count;
    EngineLockTarget targets[ENGINE_MAX_TABLES_PER_PLAN];
} LockPlan;

typedef struct {
    unsigned long long queue_wait_us;
    unsigned long long lock_wait_us;
    unsigned long long exec_us;
    unsigned long long total_us;
} ExecutionStats;

typedef struct MemoryPoolNode {
    struct MemoryPoolNode *next;
} MemoryPoolNode;

typedef struct {
    db_mutex_t mutex;
    MemoryPoolNode *free_list;
    size_t object_size;
    unsigned long long total_allocated;
    unsigned long long current_in_use;
    unsigned long long peak_in_use;
} MemoryPool;

typedef struct {
    CmdRequest request;
    char *sql_buffer;
    int in_use;
    int next_free;
} RequestSlot;

typedef struct {
    CmdResponse response;
    char *message_buffer;
    int in_use;
    int next_free;
} ResponseSlot;

typedef struct {
    void **items;
    int capacity;
    int count;
    int head;
    int tail;
    int shutdown;
    db_mutex_t mutex;
    db_cond_t not_empty;
    db_cond_t not_full;
} WorkQueue;

typedef struct {
    char template_sql[1024];
    RoutePlan route_plan;
    LockPlan lock_plan;
    unsigned long long last_used;
    int in_use;
} PlannerCacheEntry;

typedef struct {
    db_mutex_t mutex;
    PlannerCacheEntry *entries;
    int capacity;
    unsigned long long clock;
} PlannerCache;

typedef struct {
    char name[256];
    db_rwlock_t rwlock;
    int in_use;
} LockBucket;

typedef struct {
    db_mutex_t mutex;
    LockBucket buckets[ENGINE_LOCK_BUCKETS];
} LockManager;

typedef struct {
    LockBucket *bucket;
    EngineLockMode mode;
    unsigned long long wait_us;
    int acquired;
} LockHandle;

typedef struct {
    db_mutex_t mutex;
    EngineCmdProcessorStats values;
} MetricsSink;

struct CmdJob {
    CmdProcessor *processor;
    CmdRequest *request;
    ResponseSlot *response_slot;
    CmdProcessorResponseCallback callback;
    void *user_data;
    RoutePlan route_plan;
    LockPlan lock_plan;
    ExecutionStats stats;
    unsigned long long enqueue_us;
    int enqueue_depth;
};

typedef struct {
    CmdProcessor processor;
    CmdProcessorContext context;
    EngineCmdProcessorOptions options;
    db_mutex_t state_mutex;
    db_mutex_t request_mutex;
    db_mutex_t response_mutex;
    db_mutex_t engine_mutex;
    int running;
    RequestSlot *request_slots;
    ResponseSlot *response_slots;
    size_t request_count;
    size_t response_count;
    int request_free_head;
    int response_free_head;
    size_t max_sql_len;
    size_t response_body_capacity;
    unsigned long long reserved_bytes;
    unsigned long long current_request_slots_in_use;
    unsigned long long current_response_slots_in_use;
    unsigned long long peak_request_slots_in_use;
    unsigned long long peak_response_slots_in_use;
    WorkQueue *queues;
    db_thread_t *threads;
    int thread_count;
    MemoryPool job_pool;
    PlannerCache planner_cache;
    LockManager lock_manager;
    MetricsSink metrics;
} EngineCmdProcessorState;

typedef struct {
    EngineCmdProcessorState *state;
    int worker_id;
    int shard_id;
} WorkerArgs;

uint64_t monotonic_us(void);
unsigned long hash_text(const char *text);
void copy_fixed(char *dst, size_t dst_size, const char *src);
void reset_request_slot(RequestSlot *slot);
void reset_response_slot(ResponseSlot *slot);
EngineCmdProcessorState *state_from_context(CmdProcessorContext *context);
EngineCmdProcessorState *state_from_processor(CmdProcessor *processor);
int memory_pool_init(MemoryPool *pool, size_t object_size);
int memory_pool_prewarm(MemoryPool *pool, int count);
void memory_pool_destroy(MemoryPool *pool);
CmdJob *acquire_job(EngineCmdProcessorState *state);
void release_job(EngineCmdProcessorState *state, CmdJob *job);
void destroy_job_pool(MemoryPool *pool);
int work_queue_init(WorkQueue *queue, int capacity);
void work_queue_destroy(WorkQueue *queue);
int work_queue_push(WorkQueue *queue, void *item, int *depth_out);
void *work_queue_pop(WorkQueue *queue);
void work_queue_shutdown(WorkQueue *queue);
int planner_cache_init(PlannerCache *cache, int capacity);
void planner_cache_destroy(PlannerCache *cache);
int planner_cache_lookup(PlannerCache *cache, const char *sql, RoutePlan *route_plan, LockPlan *lock_plan);
int planner_cache_store(PlannerCache *cache, const char *sql, const RoutePlan *route_plan, const LockPlan *lock_plan);
int build_request_plan(EngineCmdProcessorState *state, CmdRequest *request, RoutePlan *route_plan, LockPlan *lock_plan);
int lock_manager_init(LockManager *manager);
void lock_manager_destroy(LockManager *manager);
int lock_manager_acquire(LockManager *manager, const LockPlan *plan, LockHandle *handles, unsigned long long *wait_us);
void lock_manager_release(LockManager *manager, const LockPlan *plan, LockHandle *handles);
int metrics_init(MetricsSink *sink);
void metrics_destroy(MetricsSink *sink);
void metrics_record(MetricsSink *sink, const ExecutionStats *stats, int ok, CmdStatusCode status, int queue_depth);
int acquire_response_slot(EngineCmdProcessorState *state, ResponseSlot **out_slot);
RequestSlot *request_slot_from_request(EngineCmdProcessorState *state, CmdRequest *request);
ResponseSlot *response_slot_from_response(EngineCmdProcessorState *state, CmdResponse *response);
void set_response_error(ResponseSlot *slot, const char *request_id, CmdStatusCode status, const char *message, size_t error_capacity);
void set_response_body(ResponseSlot *slot, const char *request_id, CmdBodyFormat body_format, const char *body, size_t body_len, size_t body_capacity);
int engine_execute_sql(EngineCmdProcessorState *state, CmdRequest *request, ResponseSlot *response_slot);
db_thread_return_t DB_THREAD_CALL worker_main(void *arg_ptr);

#endif
