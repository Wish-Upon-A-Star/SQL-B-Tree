#ifndef ENGINE_CMD_PROCESSOR_H
#define ENGINE_CMD_PROCESSOR_H

#include "cmd_processor.h"

typedef struct {
    int worker_count;
    int shard_count;
    int queue_capacity_per_shard;
    int planner_cache_capacity;
} EngineCmdProcessorOptions;

typedef struct {
    unsigned long long total_requests;
    unsigned long long total_errors;
    unsigned long long total_overload;
    unsigned long long total_queue_wait_us;
    unsigned long long total_lock_wait_us;
    unsigned long long total_exec_us;
    unsigned long long max_queue_wait_us;
    unsigned long long max_lock_wait_us;
    unsigned long long max_exec_us;
    int max_queue_depth;
    unsigned long long reserved_bytes;
    unsigned long long peak_request_slots_in_use;
    unsigned long long peak_response_slots_in_use;
    unsigned long long peak_jobs_in_use;
    unsigned long long peak_jobs_allocated;
    unsigned long long max_concurrent_executions;
} EngineCmdProcessorStats;

int engine_cmd_processor_create(const CmdProcessorContext *base_context,
                                const EngineCmdProcessorOptions *options,
                                CmdProcessor **out_processor);
int engine_cmd_processor_snapshot_stats(CmdProcessor *processor,
                                        EngineCmdProcessorStats *out_stats);

#endif
