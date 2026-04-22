import argparse
import asyncio
import csv
import json
import random
import socket
import struct
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path


HEADER_FIELDS = [
    "id",
    "email",
    "phone",
    "name",
    "track",
    "background",
    "history",
    "pretest",
    "github",
    "status",
    "round",
]

TRACK_VALUES = [
    "game_lab",
    "game_tech_lab",
    "sw_ai_lab",
    "engine_platform_lab",
]

STATUS_VALUES = [
    "submitted",
    "pretest_pass",
    "interview_wait",
    "final_wait",
    "final_pass",
    "rejected",
    "withdrawn",
]


def run_command(command, cwd, check=True, capture_output=False):
    return subprocess.run(
        command,
        cwd=str(cwd),
        check=check,
        text=True,
        capture_output=capture_output,
    )


def build_row(request_index, row_id):
    return {
        "id": str(row_id),
        "email": f"tcpmix{request_index:05d}_{row_id}@apply.kr",
        "phone": f"019-{(row_id // 10000) % 10000:04d}-{row_id % 10000:04d}",
        "name": f"TcpMixName{request_index:05d}",
        "track": TRACK_VALUES[request_index % len(TRACK_VALUES)],
        "background": "tcp_mix",
        "history": f"tcp_history_{request_index % 97:02d}",
        "pretest": str(30 + (request_index % 71)),
        "github": f"tcpmix_gh_{row_id}",
        "status": STATUS_VALUES[request_index % len(STATUS_VALUES)],
        "round": "2026_tcp",
    }


def format_insert_sql(table_name, row):
    values = [
        row["id"],
        f"'{row['email']}'",
        f"'{row['phone']}'",
        f"'{row['name']}'",
        f"'{row['track']}'",
        f"'{row['background']}'",
        f"'{row['history']}'",
        row["pretest"],
        f"'{row['github']}'",
        f"'{row['status']}'",
        f"'{row['round']}'",
    ]
    return f"INSERT INTO {table_name} VALUES ({','.join(values)});"


def sample_dataset_rows(dataset_path, reservoir_size=4096):
    sample = []
    sample_with_github = []
    row_count = 0
    with dataset_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise RuntimeError(f"empty dataset: {dataset_path}")
        for row in reader:
            row_count += 1
            if len(row) < len(HEADER_FIELDS):
                continue
            record = dict(zip(HEADER_FIELDS, row[: len(HEADER_FIELDS)]))
            if len(sample) < reservoir_size:
                sample.append(record)
            else:
                slot = random.randrange(row_count)
                if slot < reservoir_size:
                    sample[slot] = record
    for record in sample:
        if record.get("github") and record["github"] != "none":
            sample_with_github.append(record)
    if not sample:
        raise RuntimeError(f"dataset has no rows: {dataset_path}")
    if not sample_with_github:
        sample_with_github = sample[:]
    return sample, sample_with_github


def build_operations(table_name, dataset_size, request_count, sample_rows, github_rows, seed):
    rng = random.Random(seed)
    next_insert_id = dataset_size + 100000
    operations = []
    weights = [
        ("select_pk", 18),
        ("select_email", 10),
        ("select_phone", 10),
        ("select_github", 8),
        ("select_range", 6),
        ("insert_unique", 10),
        ("insert_dup_pk", 6),
        ("insert_dup_email", 4),
        ("update_plain_id", 8),
        ("update_plain_github", 5),
        ("update_dup_email", 5),
        ("update_dup_phone", 3),
        ("delete_id", 4),
        ("delete_email", 1),
        ("delete_github", 2),
    ]
    op_names = [name for name, _ in weights]
    op_weights = [weight for _, weight in weights]

    for request_index in range(request_count):
        kind = rng.choices(op_names, weights=op_weights, k=1)[0]
        base = rng.choice(sample_rows)
        alt = rng.choice(sample_rows)
        github_base = rng.choice(github_rows)
        request_id = f"{table_name}-{request_index:05d}"
        sql = ""
        allow_processing_error = False

        if kind == "select_pk":
            sql = f"SELECT id, email, status FROM {table_name} WHERE id = {base['id']};"
        elif kind == "select_email":
            sql = f"SELECT id, phone FROM {table_name} WHERE email = '{base['email']}';"
        elif kind == "select_phone":
            sql = f"SELECT id, track FROM {table_name} WHERE phone = '{base['phone']}';"
        elif kind == "select_github":
            sql = f"SELECT id, github, name FROM {table_name} WHERE github = '{github_base['github']}';"
        elif kind == "select_range":
            start = max(1, int(base["id"]) - rng.randint(0, 2))
            end = min(dataset_size + request_count + 100000, start + rng.randint(1, 3))
            sql = f"SELECT id, email, phone FROM {table_name} WHERE id BETWEEN {start} AND {end};"
        elif kind == "insert_unique":
            row = build_row(request_index, next_insert_id)
            next_insert_id += 1
            sql = format_insert_sql(table_name, row)
        elif kind == "insert_dup_pk":
            row = build_row(request_index, next_insert_id)
            row["id"] = base["id"]
            sql = format_insert_sql(table_name, row)
            allow_processing_error = True
            next_insert_id += 1
        elif kind == "insert_dup_email":
            row = build_row(request_index, next_insert_id)
            row["email"] = base["email"]
            sql = format_insert_sql(table_name, row)
            allow_processing_error = True
            next_insert_id += 1
        elif kind == "update_plain_id":
            sql = (
                f"UPDATE {table_name} SET status = '{STATUS_VALUES[request_index % len(STATUS_VALUES)]}' "
                f"WHERE id = {base['id']};"
            )
        elif kind == "update_plain_github":
            sql = (
                f"UPDATE {table_name} SET track = '{TRACK_VALUES[request_index % len(TRACK_VALUES)]}' "
                f"WHERE github = '{github_base['github']}';"
            )
        elif kind == "update_dup_email":
            while alt["id"] == base["id"]:
                alt = rng.choice(sample_rows)
            sql = f"UPDATE {table_name} SET email = '{alt['email']}' WHERE id = {base['id']};"
            allow_processing_error = True
        elif kind == "update_dup_phone":
            while alt["id"] == base["id"]:
                alt = rng.choice(sample_rows)
            sql = f"UPDATE {table_name} SET phone = '{alt['phone']}' WHERE id = {base['id']};"
            allow_processing_error = True
        elif kind == "delete_id":
            sql = f"DELETE FROM {table_name} WHERE id = {base['id']};"
        elif kind == "delete_email":
            sql = f"DELETE FROM {table_name} WHERE email = '{base['email']}';"
        else:
            sql = f"DELETE FROM {table_name} WHERE github = '{github_base['github']}';"

        operations.append(
            {
                "id": request_id,
                "kind": kind,
                "sql": sql,
                "allow_processing_error": allow_processing_error,
            }
        )
    return operations


class TCPConnection:
    def __init__(self, host, port, index):
        self.host = host
        self.port = port
        self.index = index
        self.reader = None
        self.writer = None
        self.pending = {}
        self.write_lock = asyncio.Lock()
        self.read_task = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.read_task = asyncio.create_task(self._read_loop())

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        if self.read_task is not None:
            self.read_task.cancel()
            try:
                await self.read_task
            except Exception:
                pass

    async def _read_loop(self):
        while True:
            line = await self.reader.readline()
            if not line:
                raise RuntimeError(f"tcp connection {self.index} closed unexpectedly")
            response = json.loads(line.decode("utf-8"))
            response_id = response.get("id")
            future = self.pending.pop(response_id, None)
            if future is not None and not future.done():
                future.set_result(response)

    async def request(self, payload):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending[payload["id"]] = future
        encoded = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        async with self.write_lock:
            self.writer.write(encoded)
            await self.writer.drain()
        return await future


def validate_active_state(table_csv, table_delta):
    active_rows = {}
    with table_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) < 3:
                continue
            row_id = int(row[0])
            active_rows[row_id] = {
                "email": row[1],
                "phone": row[2],
            }

    if table_delta.exists():
        in_batch = False
        ops = []
        with table_delta.open("r", encoding="utf-8", errors="ignore") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\r\n")
                if line == "B":
                    ops = []
                    in_batch = True
                    continue
                if line == "E":
                    if in_batch:
                        for op_type, row_id, payload in ops:
                            if op_type == "U":
                                active_rows[row_id] = payload
                            elif op_type == "D":
                                active_rows.pop(row_id, None)
                    ops = []
                    in_batch = False
                    continue
                if not in_batch:
                    continue
                if line.startswith("U\t"):
                    parts = line.split("\t", 2)
                    if len(parts) != 3:
                        continue
                    _, id_text, row_text = parts
                    parsed = next(csv.reader([row_text]))
                    if len(parsed) < 3:
                        continue
                    ops.append(
                        (
                            "U",
                            int(id_text),
                            {
                                "email": parsed[1],
                                "phone": parsed[2],
                            },
                        )
                    )
                elif line.startswith("D\t"):
                    _, id_text = line.split("\t", 1)
                    ops.append(("D", int(id_text), None))

    duplicate_emails = []
    duplicate_phones = []
    seen_emails = {}
    seen_phones = {}
    for row_id, row in active_rows.items():
        email = row["email"]
        phone = row["phone"]
        if email in seen_emails:
            duplicate_emails.append((email, seen_emails[email], row_id))
        else:
            seen_emails[email] = row_id
        if phone in seen_phones:
            duplicate_phones.append((phone, seen_phones[phone], row_id))
        else:
            seen_phones[phone] = row_id

    return {
        "active_rows": len(active_rows),
        "duplicate_email_count": len(duplicate_emails),
        "duplicate_phone_count": len(duplicate_phones),
        "duplicate_emails": duplicate_emails[:10],
        "duplicate_phones": duplicate_phones[:10],
    }


def wait_for_server(host, port, timeout_sec):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0) as sock:
                sock.sendall(b'{"id":"ping","op":"ping"}\n')
                response = sock.recv(4096)
                if response:
                    return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"server did not become ready on {host}:{port}")


def start_server(repo_root, container_name, host_port, workers, shards, queue_capacity, planner_cache):
    volume = f"{repo_root}:/app"
    command = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{host_port}:15432",
        "-v",
        volume,
        "-w",
        "/app",
        "sqlprocessor:local",
        "sh",
        "-lc",
        (
            "make tcp-server && "
            f"./tcp_sql_server --host 0.0.0.0 --port 15432 "
            f"--workers {workers} --shards {shards} "
            f"--queue-capacity {queue_capacity} --planner-cache {planner_cache}"
        ),
    ]
    run_command(command, repo_root)
    wait_for_server("127.0.0.1", host_port, 60)


def stop_server(repo_root, container_name):
    run_command(["docker", "stop", container_name], repo_root, check=False, capture_output=True)


async def run_workload(host, port, operations, connection_count, concurrency):
    connections = [TCPConnection(host, port, i) for i in range(connection_count)]
    stats = {
        "total": len(operations),
        "ok": 0,
        "errors": 0,
        "unexpected_processing_errors": 0,
        "unexpected_statuses": Counter(),
        "operation_errors": Counter(),
        "status_counts": Counter(),
        "p95_ms": 0.0,
        "elapsed_sec": 0.0,
        "throughput_rps": 0.0,
    }
    latencies_ms = []
    completed = 0
    progress_interval = max(250, len(operations) // 20)
    start = time.perf_counter()
    for connection in connections:
        await connection.connect()

    semaphore = asyncio.Semaphore(concurrency)

    async def execute(index, operation):
        nonlocal completed
        payload = {"id": operation["id"], "op": "sql", "sql": operation["sql"]}
        connection = connections[index % len(connections)]
        request_start = time.perf_counter()
        async with semaphore:
            response = await connection.request(payload)
        latency_ms = (time.perf_counter() - request_start) * 1000.0
        latencies_ms.append(latency_ms)
        status = response.get("status", "UNKNOWN")
        stats["status_counts"][status] += 1
        if response.get("ok"):
            stats["ok"] += 1
        else:
            stats["errors"] += 1
            stats["operation_errors"][operation["kind"]] += 1
            if status == "PROCESSING_ERROR" and not operation["allow_processing_error"]:
                stats["unexpected_processing_errors"] += 1
            elif status != "PROCESSING_ERROR":
                stats["unexpected_statuses"][status] += 1
        completed += 1
        if completed == len(operations) or completed % progress_interval == 0:
            elapsed = time.perf_counter() - start
            throughput = completed / elapsed if elapsed > 0 else 0.0
            print(
                f"[progress] completed={completed}/{len(operations)} "
                f"ok={stats['ok']} errors={stats['errors']} "
                f"throughput_rps={throughput:.2f}",
                flush=True,
            )

    try:
        await asyncio.gather(*(execute(i, operation) for i, operation in enumerate(operations)))
    finally:
        for connection in connections:
            await connection.close()

    elapsed_sec = time.perf_counter() - start
    latencies_ms.sort()
    if latencies_ms:
        stats["p95_ms"] = latencies_ms[min(len(latencies_ms) - 1, (len(latencies_ms) * 95 + 99) // 100 - 1)]
    stats["elapsed_sec"] = elapsed_sec
    stats["throughput_rps"] = len(operations) / elapsed_sec if elapsed_sec > 0 else 0.0
    stats["status_counts"] = dict(stats["status_counts"])
    stats["unexpected_statuses"] = dict(stats["unexpected_statuses"])
    stats["operation_errors"] = dict(stats["operation_errors"])
    return stats


async def run_warmup(host, port, table_name, sample_rows, github_rows):
    base = sample_rows[0]
    github_base = github_rows[0]
    connection = TCPConnection(host, port, 0)
    warmups = [
        f"SELECT id, email FROM {table_name} WHERE id = {base['id']};",
        f"SELECT id, phone FROM {table_name} WHERE email = '{base['email']}';",
        f"SELECT id, track FROM {table_name} WHERE phone = '{base['phone']}';",
        f"SELECT id, github, name FROM {table_name} WHERE github = '{github_base['github']}';",
    ]
    try:
        await connection.connect()
        for index, sql in enumerate(warmups):
            await connection.request({"id": f"warmup-{index}", "op": "sql", "sql": sql})
    finally:
        await connection.close()


def generate_dataset(repo_root, sql_exe, row_count, output_csv):
    run_command([str(sql_exe), "--generate-jungle", str(row_count), str(output_csv)], repo_root)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, indent=2), encoding="utf-8")


def normalize_header_name(name):
    return name.split("(", 1)[0].strip()


def ensure_aux_github_snapshot(table_csv, table_idx):
    if not table_csv.exists() or not table_idx.exists():
        return

    with table_idx.open("r", encoding="utf-8") as handle:
        lines = [line.rstrip("\r\n") for line in handle]

    aux_line_index = next((index for index, line in enumerate(lines) if line.startswith("AUXGITHUB ")), -1)
    if aux_line_index >= 0:
        parts = lines[aux_line_index].split()
        if len(parts) == 3 and parts[2] != "-1":
            return
        if aux_line_index < len(lines) - 1:
            lines = lines[:aux_line_index] + lines[-1:]
        else:
            return
    if not lines or lines[-1] != "END":
        return

    slot_header_index = -1
    slot_count = 0
    for index, line in enumerate(lines):
        if line.startswith("SLOTS "):
            slot_header_index = index
            slot_count = int(line.split()[1])
            break
    if slot_header_index < 0:
        return

    active_slots = []
    for line in lines[slot_header_index + 1 : slot_header_index + 1 + slot_count]:
        parts = line.split("\t")
        if len(parts) < 2:
            return
        if parts[1] == "1":
            active_slots.append(int(parts[0]))

    with table_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return
        normalized = [normalize_header_name(name) for name in header]
        if "github" not in normalized:
            return
        github_idx = normalized.index("github")
        pairs = []
        active_index = 0
        for row in reader:
            if active_index >= len(active_slots):
                break
            if len(row) <= github_idx:
                active_index += 1
                continue
            key = row[github_idx].strip().strip("'")
            if key and key != "none":
                pairs.append((key, active_slots[active_index]))
            active_index += 1

    pairs.sort(key=lambda item: item[0])
    github_snapshot_count = len(pairs)
    for index in range(1, len(pairs)):
        if pairs[index - 1][0] == pairs[index][0]:
            github_snapshot_count = -1
            break

    new_lines = lines[:-1]
    new_lines.append(f"AUXGITHUB {github_idx} {github_snapshot_count}")
    if github_snapshot_count > 0:
        for key, row_index in pairs:
            new_lines.append(f"{row_index}\t{key}")
    new_lines.append("END")

    temp_path = table_idx.with_suffix(".idx.tmp")
    temp_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    temp_path.replace(table_idx)


def ensure_binary_index_snapshot(table_idx):
    table_idx = Path(table_idx)
    table_idxb = table_idx.with_suffix(".idxb")
    if not table_idx.exists():
        return
    if table_idxb.exists() and table_idxb.stat().st_mtime >= table_idx.stat().st_mtime:
        return

    with table_idx.open("r", encoding="utf-8") as handle:
        lines = [line.rstrip("\r\n") for line in handle]

    if not lines or lines[0] != "SQLPROC_IDX_V2":
        return

    cursor = 1
    if cursor >= len(lines) or not lines[cursor].startswith("TABLE "):
        return
    cursor += 1
    if cursor >= len(lines) or not lines[cursor].startswith("CSV "):
        return
    cursor += 1
    if cursor >= len(lines) or not lines[cursor].startswith("DELTA "):
        return
    cursor += 1
    schema_parts = lines[cursor].split()
    cursor += 1
    if len(schema_parts) < 4 or schema_parts[0] != "SCHEMA":
        return
    col_count = int(schema_parts[1])
    pk_idx = int(schema_parts[2])
    uk_count = int(schema_parts[3])
    uk_indices = [int(value) for value in schema_parts[4:4 + uk_count]]
    while len(uk_indices) < 5:
        uk_indices.append(-1)

    row_parts = lines[cursor].split()
    cursor += 1
    if len(row_parts) != 7 or row_parts[0] != "ROWS":
        return
    record_count = int(row_parts[1])
    active_count = int(row_parts[2])
    cache_truncated = int(row_parts[3])
    tail_count = int(row_parts[4])
    next_auto_id = int(row_parts[5])
    next_row_id = int(row_parts[6])

    slots_header = lines[cursor].split()
    cursor += 1
    if len(slots_header) != 2 or slots_header[0] != "SLOTS":
        return
    slot_count = int(slots_header[1])
    slots = []
    for _ in range(slot_count):
        parts = lines[cursor].split("\t")
        cursor += 1
        slots.append(
            (
                int(parts[1]),
                int(parts[3]),
                int(parts[2]),
                int(parts[4]),
            )
        )

    id_header = lines[cursor].split()
    cursor += 1
    if len(id_header) != 2 or id_header[0] != "ID":
        return
    id_count = int(id_header[1])
    id_pairs = []
    for _ in range(id_count):
        key_text, row_index_text = lines[cursor].split("\t")
        cursor += 1
        id_pairs.append((int(key_text), int(row_index_text)))

    uk_sections = lines[cursor].split()
    cursor += 1
    if len(uk_sections) != 2 or uk_sections[0] != "UKSECTIONS":
        return
    parsed_uk_sections = []
    for _ in range(int(uk_sections[1])):
        uk_header = lines[cursor].split()
        cursor += 1
        col_idx = int(uk_header[1])
        count = int(uk_header[2])
        pairs = []
        for _ in range(count):
            row_index_text, key = lines[cursor].split("\t", 1)
            cursor += 1
            pairs.append((int(row_index_text), key))
        parsed_uk_sections.append((col_idx, pairs))

    github_idx = -1
    github_state = 0
    github_pairs = []
    if cursor < len(lines) and lines[cursor].startswith("AUXGITHUB "):
        aux_parts = lines[cursor].split()
        cursor += 1
        github_idx = int(aux_parts[1])
        aux_count = int(aux_parts[2])
        if aux_count < 0:
            github_state = -1
        elif aux_count == 0:
            github_state = 0
        else:
            github_state = 1
            for _ in range(aux_count):
                row_index_text, key = lines[cursor].split("\t", 1)
                cursor += 1
                github_pairs.append((int(row_index_text), key))

    if cursor >= len(lines) or lines[cursor] != "END":
        return

    with table_idxb.open("wb") as handle:
        handle.write(b"SQLIDXB1")
        handle.write(struct.pack("<iii", col_count, pk_idx, uk_count))
        handle.write(struct.pack("<" + "i" * 5, *uk_indices[:5]))
        handle.write(struct.pack("<iiiiqqii", record_count, active_count, cache_truncated, tail_count, next_auto_id, next_row_id, github_idx, github_state))
        handle.write(struct.pack("<i", slot_count))
        for active, store, row_id, offset in slots:
            handle.write(struct.pack("<BBHqq", active, store, 0, row_id, offset))
        handle.write(struct.pack("<i", id_count))
        for key, row_index in id_pairs:
            handle.write(struct.pack("<qi", key, row_index))
        for col_idx, pairs in parsed_uk_sections:
            handle.write(struct.pack("<ii", col_idx, len(pairs)))
            for row_index, key in pairs:
                encoded = key.encode("utf-8")
                handle.write(struct.pack("<iH", row_index, len(encoded)))
                handle.write(encoded)
        handle.write(struct.pack("<ii", github_idx, github_state))
        if github_state > 0:
            handle.write(struct.pack("<i", len(github_pairs)))
            for row_index, key in github_pairs:
                encoded = key.encode("utf-8")
                handle.write(struct.pack("<iH", row_index, len(encoded)))
                handle.write(encoded)


def build_binary_index_snapshot_from_csv(table_csv):
    table_csv = Path(table_csv)
    table_idxb = table_csv.with_suffix(".idxb")
    if table_idxb.exists() and table_idxb.stat().st_mtime >= table_csv.stat().st_mtime:
        return

    with table_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return
        names = [normalize_header_name(name) for name in header]
        col_count = len(names)
        pk_idx = names.index("id")
        email_idx = names.index("email")
        phone_idx = names.index("phone")
        github_idx = names.index("github") if "github" in names else -1

        slot_count = 0
        active_count = 0
        next_auto_id = 1
        next_row_id = 1
        id_pairs = []
        email_pairs = []
        phone_pairs = []
        github_pairs = []

        for row in reader:
            if len(row) < col_count:
                continue
            row_id = int(row[pk_idx])
            id_pairs.append((row_id, slot_count))
            email_pairs.append((row[email_idx], slot_count))
            phone_pairs.append((row[phone_idx], slot_count))
            if github_idx >= 0:
                github_value = row[github_idx].strip()
                if github_value and github_value != "none":
                    github_pairs.append((github_value, slot_count))
            slot_count += 1
            active_count += 1
            next_auto_id = max(next_auto_id, row_id + 1)
            next_row_id += 1

    id_pairs.sort(key=lambda item: item[0])
    email_pairs.sort(key=lambda item: item[0])
    phone_pairs.sort(key=lambda item: item[0])
    github_pairs.sort(key=lambda item: item[0])

    github_state = 1
    for index in range(1, len(github_pairs)):
        if github_pairs[index - 1][0] == github_pairs[index][0]:
            github_state = -1
            break

    header_line = ",".join(header)
    running_offset = len(header_line.encode("utf-8")) + 1
    row_offsets = []
    with table_csv.open("r", encoding="utf-8", newline="") as csv_handle:
        next(csv_handle)
        for line in csv_handle:
            raw = line.rstrip("\r\n")
            row_offsets.append(running_offset)
            running_offset += len(raw.encode("utf-8")) + 1

    with table_idxb.open("wb") as handle:
        handle.write(b"SQLIDXB1")
        handle.write(struct.pack("<iii", col_count, pk_idx, 2))
        handle.write(struct.pack("<iiiii", email_idx, phone_idx, -1, -1, -1))
        handle.write(struct.pack("<iiiiqqii", slot_count, active_count, 0, 0, next_auto_id, next_row_id, github_idx, github_state))
        handle.write(struct.pack("<i", slot_count))
        for offset in row_offsets:
            handle.write(struct.pack("<BBHqq", 1, 1, 0, 0, offset))
        handle.write(struct.pack("<i", len(id_pairs)))
        for key, row_index in id_pairs:
            handle.write(struct.pack("<qi", key, row_index))
        for col_idx, pairs in ((email_idx, email_pairs), (phone_idx, phone_pairs)):
            handle.write(struct.pack("<ii", col_idx, len(pairs)))
            for key, row_index in pairs:
                encoded = key.encode("utf-8")
                handle.write(struct.pack("<iH", row_index, len(encoded)))
                handle.write(encoded)
        handle.write(struct.pack("<ii", github_idx, github_state))
        if github_state > 0:
            handle.write(struct.pack("<i", len(github_pairs)))
            for key, row_index in github_pairs:
                encoded = key.encode("utf-8")
                handle.write(struct.pack("<iH", row_index, len(encoded)))
                handle.write(encoded)


def build_markdown_report(results):
    lines = ["# TCP Mixed Workload Report", ""]
    for result in results:
        lines.append(f"## {result['table_name']}")
        lines.append("")
        lines.append(f"- rows: `{result['dataset_rows']}`")
        lines.append(f"- requests: `{result['request_count']}`")
        lines.append(f"- ok: `{result['workload']['ok']}`")
        lines.append(f"- errors: `{result['workload']['errors']}`")
        lines.append(f"- unexpected_processing_errors: `{result['workload']['unexpected_processing_errors']}`")
        lines.append(f"- unexpected_statuses: `{result['workload']['unexpected_statuses']}`")
        lines.append(f"- throughput_rps: `{result['workload']['throughput_rps']:.2f}`")
        lines.append(f"- p95_ms: `{result['workload']['p95_ms']:.2f}`")
        lines.append(f"- duplicate_email_count: `{result['validation']['duplicate_email_count']}`")
        lines.append(f"- duplicate_phone_count: `{result['validation']['duplicate_phone_count']}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def resolve_sql_executable(repo_root, override):
    if override:
        return Path(override)
    for candidate in ("sqlsprocessor.exe", "sqlsprocessor"):
        path = repo_root / candidate
        if path.exists():
            return path
    raise RuntimeError("sqlsprocessor executable not found")


def parse_args():
    parser = argparse.ArgumentParser(description="Run mixed CRUD workload over TCP against SQLprocessor")
    parser.add_argument("--rows", type=int, nargs="+", default=[10000, 100000, 1000000])
    parser.add_argument("--request-count", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20260422)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15432)
    parser.add_argument("--connections", type=int, default=16)
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--shards", type=int, default=32)
    parser.add_argument("--queue-capacity", type=int, default=2048)
    parser.add_argument("--planner-cache", type=int, default=4096)
    parser.add_argument("--artifacts-dir", default="artifacts/tcp_mixed")
    parser.add_argument("--sql-exe")
    parser.add_argument("--skip-build-image", action="store_true")
    parser.add_argument("--reuse-existing", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    artifacts_dir = repo_root / args.artifacts_dir
    sql_exe = resolve_sql_executable(repo_root, args.sql_exe)
    results = []

    if not args.skip_build_image:
        run_command(["docker", "build", "-t", "sqlprocessor:local", "."], repo_root)

    for index, row_count in enumerate(args.rows):
        table_name = f"tcp_mix_{row_count}"
        table_csv = repo_root / f"{table_name}.csv"
        table_delta = repo_root / f"{table_name}.delta"
        table_idx = repo_root / f"{table_name}.idx"
        result_dir = artifacts_dir / table_name
        container_name = f"tcp-mixed-{row_count}"

        if args.reuse_existing and table_csv.exists():
            print(f"[dataset] reusing existing rows={row_count} table={table_name}", flush=True)
        else:
            for target in (table_csv, table_delta, table_idx):
                if target.exists():
                    target.unlink()
            idxb = table_csv.with_suffix(".idxb")
            if idxb.exists():
                idxb.unlink()
            print(f"[dataset] generating rows={row_count} table={table_name}", flush=True)
            generate_dataset(repo_root, sql_exe, row_count, table_csv)
        ensure_aux_github_snapshot(table_csv, table_idx)
        ensure_binary_index_snapshot(table_idx)
        build_binary_index_snapshot_from_csv(table_csv)
        random.seed(args.seed + index)
        sample_rows, github_rows = sample_dataset_rows(table_csv)
        operations = build_operations(
            table_name,
            row_count,
            args.request_count,
            sample_rows,
            github_rows,
            args.seed + index,
        )
        write_json(result_dir / "workload.json", operations)

        stop_server(repo_root, container_name)
        print(f"[dataset] starting tcp server rows={row_count}", flush=True)
        start_server(
            repo_root,
            container_name,
            args.port,
            args.workers,
            args.shards,
            args.queue_capacity,
            args.planner_cache,
        )
        try:
            print(f"[dataset] warming up rows={row_count}", flush=True)
            asyncio.run(run_warmup(args.host, args.port, table_name, sample_rows, github_rows))
            print(f"[dataset] running workload rows={row_count} requests={args.request_count}", flush=True)
            workload_stats = asyncio.run(
                run_workload(args.host, args.port, operations, args.connections, args.concurrency)
            )
        finally:
            stop_server(repo_root, container_name)

        validation = validate_active_state(table_csv, table_delta)
        result = {
            "table_name": table_name,
            "dataset_rows": row_count,
            "request_count": args.request_count,
            "workload": workload_stats,
            "validation": validation,
        }
        write_json(result_dir / "result.json", result)
        print(
            f"[dataset] completed rows={row_count} ok={workload_stats['ok']} "
            f"errors={workload_stats['errors']} throughput_rps={workload_stats['throughput_rps']:.2f} "
            f"p95_ms={workload_stats['p95_ms']:.2f}",
            flush=True,
        )
        results.append(result)

    write_json(artifacts_dir / "report.json", results)
    (artifacts_dir / "report.md").write_text(build_markdown_report(results), encoding="utf-8")

    has_failure = False
    for result in results:
        workload = result["workload"]
        validation = result["validation"]
        if workload["unexpected_processing_errors"] > 0:
            has_failure = True
        if workload["unexpected_statuses"]:
            has_failure = True
        if validation["duplicate_email_count"] > 0 or validation["duplicate_phone_count"] > 0:
            has_failure = True

    if has_failure:
        print(json.dumps(results, ensure_ascii=True, indent=2))
        return 1

    print(json.dumps(results, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
