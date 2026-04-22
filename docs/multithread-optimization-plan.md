# SQLprocessor DB + Worker Optimization Notes

## Scope

This document tracks the DB-engine and worker-thread changes that were made for the mini DBMS API server assignment.

The emphasis is intentionally on:

- worker-thread execution
- planner routing and lock behavior
- executor/index/storage efficiency
- external TCP benchmark implications

It is not a frontend protocol design document.

## Current Architecture Summary

The current runtime path is:

1. frontend (`main.c`, REPL, or TCP) creates a `CmdRequest`
2. `engine_cmd_processor` builds a route plan and lock plan
3. the request is assigned to a shard queue
4. a worker thread executes parse + executor path
5. a `CmdResponse` is returned to the frontend

The important point is that SQL now executes on worker threads as the normal path, not inline on the caller thread.

## What Changed In The Engine

### 1. `SELECT` response body is binary inside the engine

Previous state:

- `SELECT` results were serialized to JSON inside the engine
- worker threads paid string-building and JSON formatting cost

Current state:

- `CmdBodyFormat` includes `CMD_BODY_BINARY`
- `SELECT` uses a compact binary rowset body
- `INSERT/UPDATE/DELETE` still keep text summaries

Result:

- lower worker-side serialization cost
- cleaner engine/frontend boundary
- easier future binary wire protocol migration if the TCP owner decides to do it

### 2. Read requests are spread by SQL hash, not only table hash

Previous state:

- requests against the same table could accumulate on one shard queue

Current state:

- `READ` requests are routed by raw SQL hash
- `WRITE` requests still keep table-oriented routing

Result:

- lower queue hotspot probability for same-table reads
- worker utilization improves when many read queries target one table with different SQL shapes

### 3. Planner cache no longer destroys read-shard distribution

Previous state:

- normalized planner cache entries could accidentally reuse the first request's `target_shard`
- different read SQL templates could collapse back onto one shard

Current state:

- cache hits still reuse the parsed plan
- but `READ` routes recompute `target_shard` from the raw SQL

Result:

- planner cache keeps its value without breaking read distribution

### 4. Same-table read path is no longer globally serialized by file cursor sharing

Previous state:

- the executor shared mutable read state such as `FILE *` and page-cache cursor behavior
- same-table read/read concurrency was effectively unsafe

Current state:

- table-local `io_mutex` protects the narrow shared file/page-cache region
- planner uses read locks for `SELECT`
- same-table read/read workloads can overlap outside the narrow IO critical section

Result:

- same-table concurrent `SELECT` became feasible
- read/write and write/write correctness are still protected

### 5. Large-table read path was shortened

Previous state:

- large scans and repeated indexed fetches paid repeated CSV/page-cache cost

Current state:

- large cached tables can materialize rows once
- binary snapshot preload path (`.idxb`) reduces large-table startup cost

Result:

- much lower warmup/startup cost for large benchmark tables
- point lookup and mixed CRUD workloads lose less time to repeated row reconstruction

### 6. `github` exact-match auxiliary index was added

Previous state:

- hot exact-match queries on `github` fell back to scan

Current state:

- an auxiliary exact-match index is built for `github`
- snapshot support includes this auxiliary index
- the special high-duplication sentinel value `none` is excluded

Result:

- mixed CRUD workloads with `WHERE github = ...` stop paying a full scan in the common indexed case

## Concurrency Model Today

### Parallel today

- different-table requests
- same-table read/read requests
- concurrent request submission to shard queues

### Serialized today

- same-table read/write conflicts
- same-table write/write conflicts
- duplicate PK/UK insert races
- same-id update/delete mutation conflicts

This is the intended current safety boundary.

## Worker Model Evaluation

The worker model is not limited mainly by thread count anymore.

Measured behavior during external TCP benchmarking:

- `workers=32`, `shards=32` performed better than more aggressive settings
- `workers=64`, `shards=64` did not produce higher throughput

Interpretation:

- the next bottleneck is not “too few workers”
- the next bottleneck is queue contention, lock contention, and TCP/frontend overhead

So the right question is no longer “how many workers can we add”, but:

- are queues balanced well enough
- are lock boundaries too wide
- does the frontend waste the worker-side gain

## External TCP Benchmark Impact

The DB/worker changes matter only if they survive the external TCP path.

Important finding:

- external TCP was previously hiding part of the DB/worker gain because the TCP layer was expensive

That is why some TCP hot-path reductions were also necessary for benchmark honesty:

- buffered receive instead of `recv(..., 1)`
- fixed-schema manual JSON parse/serialize path
- higher connection/inflight caps

Even so, the current external benchmark limit is still above the DB/worker layer in some runs.

## Verified Scenarios

The current engine regression test covers:

- binary `SELECT` body contract
- same-table read/read concurrency
- same-id concurrent reads
- same PK concurrent insert
- same UK concurrent insert
- same-table read/write serialization
- same-id read/update serialization

Important correctness contract:

- duplicate PK/UK races are validated as “exactly one success”, not “a specific thread must win”

## Verified Commands

```powershell
gcc -O2 -fdiagnostics-color=always -g main.c -o sqlsprocessor.exe
gcc -O2 -fdiagnostics-color=always -g -Icmd_processor cmd_processor\engine_cmd_processor_test.c cmd_processor\cmd_processor.c cmd_processor\engine_cmd_processor_bundle.c lexer.c parser.c bptree.c jungle_benchmark.c executor.c -o engine_cmd_processor_test.exe -pthread
.\engine_cmd_processor_test.exe
python -m py_compile scripts\tcp_mixed_workload.py
python scripts\tcp_mixed_workload.py --rows 1000000 --request-count 10000 --skip-build-image
```

## Measured External TCP Result Snapshot

Verified fresh external TCP mixed CRUD result:

- dataset: `1,000,000 rows`
- workload: `10,000 mixed SQL`
- protocol: external TCP
- throughput: about `13.26k rps`
- p95: about `641 ms`
- duplicate email/phone after workload: `0 / 0`
- unexpected processing errors: `0`

This is an improvement over the earlier verified external result, but it still does not satisfy the `1,000,000 SQL in 60 seconds` target.

Required target:

- `16,666.67 rps`

Current gap:

- about `20%`

## What Still Looks Expensive

### DB side

- single-row `UPDATE/DELETE` still do meaningful row reconstruction and delta/index synchronization work
- mixed predicate workloads still fall back to scan for non-indexed conditions
- row materialization/copy cost is still visible on large workloads

### Worker side

- queue and lock contention become visible before “worker count shortage” does
- raising shard/worker counts alone is not enough

### TCP side

- JSON line protocol still has parse/escape/serialize cost
- response framing is still text-oriented even though engine `SELECT` is binary internally

## Practical Next Backlog

Ordered by likely impact inside the current architecture:

1. shrink single-row `UPDATE/DELETE` mutation hot path further
2. profile operation-type latency breakdown in mixed workload
3. tighten queue/lock contention rather than only raising worker count
4. let the TCP owner decide whether to keep JSONL or move to a framed binary protocol

## Team Handoff Note

If another teammate owns the TCP layer from here:

- DB/worker-side engine boundary is already prepared for binary `SELECT` bodies
- the executor and worker tests now cover duplicate and mixed-concurrency cases
- TCP protocol-level optimization can proceed independently without rewriting the DB core first

That is the main reason the current work should be merged as a stable base rather than kept as an experiment branch.
