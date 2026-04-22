import json
import os
import socket
import time
from ctypes import c_ulong, sizeof
from itertools import count
from pathlib import Path

import gevent
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


def _inc_counter(mapping, key):
    if key is None:
        return
    text_key = str(key)
    mapping[text_key] = mapping.get(text_key, 0) + 1


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
        elapsed = time.monotonic() - self.started_at
        is_success = exception is None
        expected_shard = None
        if context:
            expected_shard = context.get("expected_shard")

        if elapsed < self.warmup_seconds:
            if is_success:
                self.warmup_success += 1
            else:
                self.warmup_failures += 1
            return

        second_index = int(elapsed - self.warmup_seconds)
        if 0 <= second_index < self.measure_seconds:
            if is_success:
                self.success_by_second[second_index] += 1
                self.measure_success += 1
                _inc_counter(self.measure_success_by_expected_shard, expected_shard)
            else:
                self.failure_by_second[second_index] += 1
                self.measure_failures += 1
                _inc_counter(self.measure_failures_by_expected_shard, expected_shard)
            return

        if is_success:
            self.after_measure_success += 1
        else:
            self.after_measure_failures += 1

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
        self.reader = None

    def connect(self):
        if self.sock is not None:
            return
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.reader = self.sock.makefile("rb", buffering=0)

    def close(self):
        if self.reader is not None:
            try:
                self.reader.close()
            except OSError:
                pass
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.reader = None
        self.sock = None

    def send_batch(self, payloads):
        self.connect()
        encoded = b"".join(
            (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
            for payload in payloads
        )
        self.sock.sendall(encoded)

    def read_response(self):
        self.connect()
        line = self.reader.readline()
        if not line:
            raise RuntimeError("server closed connection")
        return json.loads(line.decode("utf-8"))


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
        if self.sql_id_max < self.sql_id_min:
            self.sql_id_max = self.sql_id_min
        self.socket_index = (self.process_index - 1) + next(USER_INDEX_COUNTER) * max(1, self.process_count)
        self.user_id = f"{id(self):x}"
        self.sequence = 0
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

    def _build_payload(self):
        request_id = f"{self.user_id}-{self.sequence}"
        request_sequence = self.sequence
        self.sequence += 1
        if self.request_op == "ping":
            return {"id": request_id, "op": "ping", "expected_shard": None}
        sql = self._build_sql(request_sequence)
        return {
            "id": request_id,
            "op": "sql",
            "sql": sql,
            "expected_shard": self._expected_shard(sql),
        }

    @task
    def send_pipelined_requests(self):
        batch_start = time.perf_counter()
        payloads = [self._build_payload() for _ in range(self.pipeline_depth)]
        sent_at = batch_start
        pending = {
            payload["id"]: {
                "started_at": sent_at,
                "expected_shard": payload.get("expected_shard"),
            }
            for payload in payloads
        }
        wire_payloads = [
            {key: value for key, value in payload.items() if key != "expected_shard"}
            for payload in payloads
        ]

        try:
            self.client.send_batch(wire_payloads)
            for _ in payloads:
                response = self.client.read_response()
                request_id = response.get("id")
                if request_id not in pending:
                    raise RuntimeError(f"unexpected response id: {response!r}")
                pending_item = pending[request_id]
                start = pending_item["started_at"]
                response_time_ms = (time.perf_counter() - start) * 1000.0
                if not response.get("ok"):
                    raise RuntimeError(f"request failed: {response!r}")
                pending.pop(request_id)
                events.request.fire(
                    request_type="TCP",
                    name=self.request_op,
                    response_time=response_time_ms,
                    response_length=0,
                    exception=None,
                    context={"expected_shard": pending_item.get("expected_shard")},
                )
        except Exception as exc:
            response_time_ms = (time.perf_counter() - sent_at) * 1000.0
            pending_items = list(pending.values()) or [{"expected_shard": None}]
            for item in pending_items:
                events.request.fire(
                    request_type="TCP",
                    name=self.request_op,
                    response_time=response_time_ms,
                    response_length=0,
                    exception=exc,
                    context={"expected_shard": item.get("expected_shard")},
            )
            self.client.close()
        finally:
            if self.batch_interval > 0.0:
                elapsed = time.perf_counter() - batch_start
                remaining = self.batch_interval - elapsed
                if remaining > 0.0:
                    gevent.sleep(remaining)
