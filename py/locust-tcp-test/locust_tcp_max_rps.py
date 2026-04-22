import json
import os
import time
from ctypes import c_ulong, sizeof
from itertools import count
from math import gcd
from pathlib import Path

import gevent
from gevent import socket as gsocket
from locust import User, events, task


def _env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def _percentile(values, percentile):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (percentile / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def _hash_text_like_c_ulong(text):
    mask = (1 << (sizeof(c_ulong) * 8)) - 1
    value = 5381
    for byte in text.encode("utf-8"):
        value = (((value << 5) + value) + byte) & mask
    return value


def _add_counter(mapping, key, count_value):
    if key is None:
        return
    if count_value <= 0:
        return
    text_key = str(key)
    mapping[text_key] = mapping.get(text_key, 0) + int(count_value)


def _add_raw_counter(mapping, key, count_value=1):
    if count_value <= 0:
        return
    mapping[key] = mapping.get(key, 0) + int(count_value)


def _count_values(values):
    counts = {}
    for value in values:
        _add_raw_counter(counts, value)
    return counts


def _subtract_counts(total_counts, subtract_counts):
    remaining = dict(total_counts)
    for key, value in subtract_counts.items():
        new_value = remaining.get(key, 0) - int(value)
        if new_value > 0:
            remaining[key] = new_value
        else:
            remaining.pop(key, None)
    return remaining


class ThroughputRecorder:
    def __init__(self):
        self.started_at = 0.0
        self.wall_started_at = ""
        self.warmup_seconds = 30
        self.measure_seconds = 60
        self.success_by_second = []
        self.failure_by_second = []
        self.warmup_success = 0
        self.warmup_failures = 0
        self.measure_success = 0
        self.measure_failures = 0
        self.after_measure_success = 0
        self.after_measure_failures = 0
        self.measure_success_by_expected_shard = {}
        self.measure_failures_by_expected_shard = {}

    def start(self):
        self.warmup_seconds = _env_int("LOCUST_TCP_WARMUP_SECONDS", 30)
        self.measure_seconds = _env_int("LOCUST_TCP_MEASURE_SECONDS", 60)
        self.success_by_second = [0 for _ in range(self.measure_seconds)]
        self.failure_by_second = [0 for _ in range(self.measure_seconds)]
        self.warmup_success = 0
        self.warmup_failures = 0
        self.measure_success = 0
        self.measure_failures = 0
        self.after_measure_success = 0
        self.after_measure_failures = 0
        self.measure_success_by_expected_shard = {}
        self.measure_failures_by_expected_shard = {}
        self.started_at = time.monotonic()
        self.wall_started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def record(self, exception, context=None):
        expected_shard = None
        if context:
            expected_shard = context.get("expected_shard")
        if exception is None:
            self.record_counts(success_counts={expected_shard: 1})
        else:
            self.record_counts(failure_counts={expected_shard: 1})

    def record_counts(self, success_counts=None, failure_counts=None):
        elapsed = time.monotonic() - self.started_at
        self._record_counts_at(elapsed, True, success_counts or {})
        self._record_counts_at(elapsed, False, failure_counts or {})

    def _record_counts_at(self, elapsed, is_success, counts_by_shard):
        total_count = sum(int(value) for value in counts_by_shard.values())
        if total_count <= 0:
            return

        if elapsed < self.warmup_seconds:
            if is_success:
                self.warmup_success += total_count
            else:
                self.warmup_failures += total_count
            return

        second_index = int(elapsed - self.warmup_seconds)
        if 0 <= second_index < self.measure_seconds:
            if is_success:
                self.success_by_second[second_index] += total_count
                self.measure_success += total_count
                for expected_shard, count_value in counts_by_shard.items():
                    _add_counter(self.measure_success_by_expected_shard, expected_shard, count_value)
            else:
                self.failure_by_second[second_index] += total_count
                self.measure_failures += total_count
                for expected_shard, count_value in counts_by_shard.items():
                    _add_counter(self.measure_failures_by_expected_shard, expected_shard, count_value)
            return

        if is_success:
            self.after_measure_success += total_count
        else:
            self.after_measure_failures += total_count

    def result(self):
        percentiles = {
            "p5": _percentile(self.success_by_second, 5),
            "p15": _percentile(self.success_by_second, 15),
            "p50": _percentile(self.success_by_second, 50),
            "p75": _percentile(self.success_by_second, 75),
            "p95": _percentile(self.success_by_second, 95),
        }
        average = (
            float(self.measure_success) / float(self.measure_seconds)
            if self.measure_seconds > 0
            else 0.0
        )
        return {
            "started_at": self.wall_started_at,
            "warmup_seconds": self.warmup_seconds,
            "measure_seconds": self.measure_seconds,
            "warmup_success": self.warmup_success,
            "warmup_failures": self.warmup_failures,
            "measure_success": self.measure_success,
            "measure_failures": self.measure_failures,
            "after_measure_success": self.after_measure_success,
            "after_measure_failures": self.after_measure_failures,
            "average_rps": average,
            "percentiles_rps": percentiles,
            "success_rps_by_second": self.success_by_second,
            "failure_rps_by_second": self.failure_by_second,
            "measure_success_by_expected_shard": self.measure_success_by_expected_shard,
            "measure_failures_by_expected_shard": self.measure_failures_by_expected_shard,
        }

    def write(self):
        result_path = os.environ.get("LOCUST_TCP_RESULT_PATH")
        if not result_path:
            return
        path = Path(result_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.result(), ensure_ascii=True, indent=2), encoding="utf-8")


RECORDER = ThroughputRecorder()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    RECORDER.start()


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    if request_type == "TCP":
        RECORDER.record(exception, kwargs.get("context"))


USER_INDEX_COUNTER = count()


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    RECORDER.write()


class SQLTCPClient:
    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.read_buffer = bytearray()

    def connect(self):
        if self.sock is not None:
            return
        self.sock = gsocket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.read_buffer.clear()

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.read_buffer.clear()

    def send_bytes(self, encoded):
        self.connect()
        self.sock.sendall(encoded)

    def read_response(self):
        self.connect()
        while True:
            newline_index = self.read_buffer.find(b"\n")
            if newline_index >= 0:
                line = bytes(self.read_buffer[: newline_index + 1])
                del self.read_buffer[: newline_index + 1]
                break
            chunk = self.sock.recv(8192)
            if not chunk:
                raise RuntimeError("server closed connection")
            self.read_buffer.extend(chunk)
        request_id = _extract_compact_json_string(line, b"id")
        if not request_id:
            raise RuntimeError(f"response id missing: {line[:256]!r}")
        return {
            "id": request_id,
            "ok": b'"ok":true' in line,
            "line": line,
        }


def _extract_compact_json_string(line, key):
    marker = b'"' + key + b'":"'
    start = line.find(marker)
    if start < 0:
        return None
    start += len(marker)
    end = line.find(b'"', start)
    if end < 0:
        return None
    return line[start:end]


class RequestPlan:
    __slots__ = ("request_id", "expected_shard", "wire")

    def __init__(self, request_id, expected_shard, wire):
        self.request_id = request_id
        self.expected_shard = expected_shard
        self.wire = wire


class BatchPlan:
    __slots__ = ("encoded", "pending_by_id", "expected_shards", "expected_shard_counts", "count")

    def __init__(self, encoded, pending_by_id, expected_shards):
        self.encoded = encoded
        self.pending_by_id = pending_by_id
        self.expected_shards = expected_shards
        self.expected_shard_counts = _count_values(expected_shards)
        self.count = len(expected_shards)


class TCPMaxRPSUser(User):
    abstract = False

    def on_start(self):
        self.host_name = os.environ.get("LOCUST_TCP_HOST", "127.0.0.1")
        self.port = _env_int("LOCUST_TCP_PORT", 15432)
        self.timeout = float(os.environ.get("LOCUST_TCP_TIMEOUT_SECONDS", "10"))
        self.pipeline_depth = _env_int("LOCUST_TCP_PIPELINE_DEPTH", 32)
        self.target_rps = _env_float("LOCUST_TCP_TARGET_RPS", 0.0)
        self.locust_users = _env_int("LOCUST_TCP_USERS", 1)
        self.server_shards = _env_int("LOCUST_SERVER_SHARDS", 1)
        self.process_index = _env_int("LOCUST_LOADGEN_PROCESS_INDEX", 1)
        self.process_count = _env_int("LOCUST_LOADGEN_PROCESS_COUNT", 1)
        self.request_op = os.environ.get("LOCUST_TCP_OP", "sql")
        self.sql = os.environ.get(
            "LOCUST_TCP_SQL",
            "SELECT * FROM case_basic_users WHERE id = 2;",
        )
        self.sql_template = os.environ.get("LOCUST_TCP_SQL_TEMPLATE", "")
        self.sql_id_min = _env_int("LOCUST_TCP_SQL_ID_MIN", 1)
        self.sql_id_max = _env_int("LOCUST_TCP_SQL_ID_MAX", self.sql_id_min)
        self.sql_variant_count = _env_int("LOCUST_TCP_SQL_VARIANT_COUNT", 0)
        self.sql_pool_size = _env_int("LOCUST_TCP_SQL_POOL_SIZE", 4096)
        if self.sql_id_max < self.sql_id_min:
            self.sql_id_max = self.sql_id_min
        if self.sql_pool_size <= 0:
            self.sql_pool_size = 1
        self.request_pool_size = max(self.sql_pool_size, self.pipeline_depth, 1)
        self.socket_index = (self.process_index - 1) + next(USER_INDEX_COUNTER) * max(1, self.process_count)
        self.user_id = f"{self.process_index:x}-{self.socket_index:x}-{id(self):x}"
        self.batch_sequence = 0
        self.batch_plans = self._prebuild_batch_plans()
        self.client = SQLTCPClient(self.host_name, self.port, self.timeout)
        self.client.connect()
        self.batch_interval = 0.0
        if self.target_rps > 0.0 and self.locust_users > 0:
            per_user_rps = self.target_rps / float(self.locust_users)
            if per_user_rps > 0.0:
                self.batch_interval = float(self.pipeline_depth) / per_user_rps

    def on_stop(self):
        self.client.close()

    def _next_sql_values(self, request_sequence):
        span = self.sql_id_max - self.sql_id_min + 1
        sequence_index = self.socket_index + request_sequence * max(1, self.locust_users)
        id_value = self.sql_id_min + (sequence_index % span)
        variant_count = max(0, self.sql_variant_count)
        variant = sequence_index % variant_count if variant_count > 0 else 0
        pad = "\t" * (variant + 1) if variant_count > 0 else ""
        return id_value, sequence_index, variant, pad

    def _build_sql(self, request_sequence):
        if not self.sql_template:
            return self.sql
        id_value, sequence_index, variant, pad = self._next_sql_values(request_sequence)
        return self.sql_template.format(
            id=id_value,
            seq=sequence_index,
            variant=variant,
            pad=pad,
        )

    def _expected_shard(self, sql):
        if self.server_shards <= 0:
            return None
        return _hash_text_like_c_ulong(sql) % self.server_shards

    def _prebuild_request_pool(self):
        plans = []
        for request_sequence in range(self.request_pool_size):
            request_id = f"{self.user_id}-{request_sequence}".encode("ascii")
            if self.request_op == "ping":
                wire = b'{"id":"' + request_id + b'","op":"ping"}\n'
                plans.append(RequestPlan(request_id, None, wire))
                continue
            sql = self._build_sql(request_sequence)
            sql_json = json.dumps(sql, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
            wire = b'{"id":"' + request_id + b'","op":"sql","sql":' + sql_json + b"}\n"
            plans.append(RequestPlan(request_id, self._expected_shard(sql), wire))
        return plans

    def _prebuild_batch_plans(self):
        request_pool = self._prebuild_request_pool()
        step = max(1, self.pipeline_depth)
        batch_count = len(request_pool) // gcd(len(request_pool), step)
        batches = []
        for batch_index in range(batch_count):
            offset = (batch_index * step) % len(request_pool)
            batch_requests = [request_pool[(offset + index) % len(request_pool)] for index in range(step)]
            pending_by_id = {plan.request_id: plan.expected_shard for plan in batch_requests}
            if len(pending_by_id) != len(batch_requests):
                raise RuntimeError("--sql-pool-size must provide unique request ids within a pipeline batch")
            batches.append(
                BatchPlan(
                    b"".join(plan.wire for plan in batch_requests),
                    pending_by_id,
                    tuple(plan.expected_shard for plan in batch_requests),
                )
            )
        return batches

    @task
    def send_pipelined_requests(self):
        batch_start = time.perf_counter()
        batch = self.batch_plans[self.batch_sequence % len(self.batch_plans)]
        self.batch_sequence += 1
        success_counts = {}

        try:
            self.client.send_bytes(batch.encoded)
            for _ in range(batch.count):
                response = self.client.read_response()
                request_id = response.get("id")
                if request_id not in batch.pending_by_id:
                    raise RuntimeError(f"unexpected response id: {response!r}")
                expected_shard = batch.pending_by_id[request_id]
                if not response.get("ok"):
                    raise RuntimeError(f"request failed: {response.get('line', b'')[:256]!r}")
                _add_raw_counter(success_counts, expected_shard)
            RECORDER.record_counts(success_counts=success_counts)
        except Exception:
            failure_counts = _subtract_counts(batch.expected_shard_counts, success_counts)
            RECORDER.record_counts(success_counts=success_counts, failure_counts=failure_counts)
            self.client.close()
        finally:
            if self.batch_interval > 0.0:
                elapsed = time.perf_counter() - batch_start
                remaining = self.batch_interval - elapsed
                if remaining > 0.0:
                    gevent.sleep(remaining)
