import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_WORKER_COUNTS = [1, 2, 4, 8, 12, 16, 24, 32]


def parse_worker_counts(text):
    if not text:
        return DEFAULT_WORKER_COUNTS
    values = []
    for token in text.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            step = 1 if end >= start else -1
            values.extend(range(start, end + step, step))
        else:
            values.append(int(token))
    return sorted(dict.fromkeys(values))


def run_command(command, cwd, check=True):
    print(f"[run] {' '.join(str(part) for part in command)}", flush=True)
    completed = subprocess.run(command, cwd=cwd)
    if check and completed.returncode != 0:
        raise RuntimeError(f"command failed with exit code {completed.returncode}: {command}")
    return completed.returncode


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=True, indent=2), encoding="utf-8")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def percentile(values, percentile_value):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (percentile_value / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def split_int(total, parts):
    if parts <= 0:
        return []
    base = total // parts
    remainder = total % parts
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def merge_count_map(target, source):
    if not source:
        return
    for key, value in source.items():
        target[str(key)] = int(target.get(str(key), 0)) + int(value)


def shard_summary(run):
    counts = run.get("measure_success_by_expected_shard") or {}
    total = sum(int(value) for value in counts.values())
    active = sum(1 for value in counts.values() if int(value) > 0)
    expected = int(run.get("server_shards") or run.get("expected_shard_count") or 0)
    max_percent = 0.0
    if total > 0 and counts:
        max_percent = max(int(value) for value in counts.values()) * 100.0 / float(total)
    return active, expected, max_percent


def wait_for_server(host, port, timeout_seconds):
    deadline = time.time() + timeout_seconds
    payload = b'{"id":"ready","op":"ping"}\n'
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0) as sock:
                sock.settimeout(1.0)
                sock.sendall(payload)
                if sock.recv(4096):
                    return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"server did not become ready on {host}:{port}")


def start_tcp_server(args, repo_root, server_workers):
    shards = args.shards if args.shards is not None else server_workers
    command = [
        str(args.server_bin),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--workers",
        str(server_workers),
        "--shards",
        str(shards),
        "--queue-capacity",
        str(args.queue_capacity),
        "--planner-cache",
        str(args.planner_cache),
    ]
    log_path = args.artifacts_dir / f"server_workers_{server_workers}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w", encoding="utf-8")
    print(f"[server] start workers={server_workers} shards={shards}", flush=True)
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_server(args.host, args.port, args.startup_timeout_seconds)
    except Exception:
        stop_process(process)
        log_file.close()
        raise
    return process, log_file, shards, log_path


def stop_process(process):
    if not process or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def build_locust_env(args,
                     server_workers,
                     shards,
                     result_path,
                     process_index,
                     tcp_client_sockets,
                     target_rps):
    env = os.environ.copy()
    env.update(
        {
            "LOCUST_TCP_HOST": args.host,
            "LOCUST_TCP_PORT": str(args.port),
            "LOCUST_TCP_WARMUP_SECONDS": str(args.warmup_seconds),
            "LOCUST_TCP_MEASURE_SECONDS": str(args.measure_seconds),
            "LOCUST_TCP_PIPELINE_DEPTH": str(args.pipeline_depth),
            "LOCUST_TCP_TARGET_RPS": str(target_rps),
            "LOCUST_TCP_USERS": str(tcp_client_sockets),
            "LOCUST_TCP_OP": args.op,
            "LOCUST_TCP_SQL": args.sql,
            "LOCUST_TCP_SQL_TEMPLATE": args.sql_template or "",
            "LOCUST_TCP_SQL_ID_MIN": str(args.sql_id_min),
            "LOCUST_TCP_SQL_ID_MAX": str(args.sql_id_max),
            "LOCUST_TCP_SQL_VARIANT_COUNT": str(args.sql_variant_count),
            "LOCUST_TCP_RESULT_PATH": str(result_path),
            "LOCUST_SERVER_WORKERS": str(server_workers),
            "LOCUST_SERVER_SHARDS": str(shards),
            "LOCUST_LOADGEN_PROCESS_INDEX": str(process_index),
            "LOCUST_LOADGEN_PROCESS_COUNT": str(args.loadgen_processes),
        }
    )
    return env


def build_locust_single_command(args, process_users, process_spawn_rate):
    run_time = args.warmup_seconds + args.measure_seconds
    project_dir = Path(__file__).resolve().parent
    locustfile = project_dir / "locust_tcp_max_rps.py"
    return [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(locustfile),
        "--headless",
        "-u",
        str(process_users),
        "-r",
        str(process_spawn_rate),
        "--run-time",
        f"{run_time}s",
        "--stop-timeout",
        str(args.stop_timeout_seconds),
        "--host",
        f"tcp://{args.host}:{args.port}",
        "--only-summary",
    ]


def build_locust_master_command(args):
    run_time = args.warmup_seconds + args.measure_seconds
    project_dir = Path(__file__).resolve().parent
    locustfile = project_dir / "locust_tcp_max_rps.py"
    return [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(locustfile),
        "--master",
        "--master-bind-host",
        "127.0.0.1",
        "--master-bind-port",
        str(args.locust_master_port),
        "--expect-workers",
        str(args.loadgen_processes),
        "--expect-workers-max-wait",
        str(args.startup_timeout_seconds),
        "--headless",
        "-u",
        str(args.locust_users),
        "-r",
        str(args.spawn_rate),
        "--run-time",
        f"{run_time}s",
        "--stop-timeout",
        str(args.stop_timeout_seconds),
        "--host",
        f"tcp://{args.host}:{args.port}",
        "--only-summary",
    ]


def build_locust_worker_command(args):
    project_dir = Path(__file__).resolve().parent
    locustfile = project_dir / "locust_tcp_max_rps.py"
    return [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(locustfile),
        "--worker",
        "--master-host",
        "127.0.0.1",
        "--master-port",
        str(args.locust_master_port),
        "--host",
        f"tcp://{args.host}:{args.port}",
    ]


def aggregate_locust_results(result_path, process_results):
    if not process_results:
        raise RuntimeError("no Locust process results to aggregate")

    first = process_results[0]
    measure_seconds = int(first["measure_seconds"])
    success_by_second = [0 for _ in range(measure_seconds)]
    failure_by_second = [0 for _ in range(measure_seconds)]
    aggregate = {
        "started_at": first.get("started_at", ""),
        "warmup_seconds": first.get("warmup_seconds", 0),
        "measure_seconds": measure_seconds,
        "warmup_success": 0,
        "warmup_failures": 0,
        "measure_success": 0,
        "measure_failures": 0,
        "after_measure_success": 0,
        "after_measure_failures": 0,
        "measure_success_by_expected_shard": {},
        "measure_failures_by_expected_shard": {},
    }

    for result in process_results:
        if int(result["measure_seconds"]) != measure_seconds:
            raise RuntimeError("Locust process result measure_seconds mismatch")
        aggregate["warmup_success"] += int(result.get("warmup_success", 0))
        aggregate["warmup_failures"] += int(result.get("warmup_failures", 0))
        aggregate["measure_success"] += int(result.get("measure_success", 0))
        aggregate["measure_failures"] += int(result.get("measure_failures", 0))
        aggregate["after_measure_success"] += int(result.get("after_measure_success", 0))
        aggregate["after_measure_failures"] += int(result.get("after_measure_failures", 0))
        merge_count_map(
            aggregate["measure_success_by_expected_shard"],
            result.get("measure_success_by_expected_shard"),
        )
        merge_count_map(
            aggregate["measure_failures_by_expected_shard"],
            result.get("measure_failures_by_expected_shard"),
        )
        for index, value in enumerate(result.get("success_rps_by_second", [])):
            if index < measure_seconds:
                success_by_second[index] += int(value)
        for index, value in enumerate(result.get("failure_rps_by_second", [])):
            if index < measure_seconds:
                failure_by_second[index] += int(value)

    aggregate["average_rps"] = (
        float(aggregate["measure_success"]) / float(measure_seconds)
        if measure_seconds > 0
        else 0.0
    )
    aggregate["percentiles_rps"] = {
        "p5": percentile(success_by_second, 5),
        "p15": percentile(success_by_second, 15),
        "p50": percentile(success_by_second, 50),
        "p75": percentile(success_by_second, 75),
        "p95": percentile(success_by_second, 95),
    }
    aggregate["success_rps_by_second"] = success_by_second
    aggregate["failure_rps_by_second"] = failure_by_second
    write_json(result_path, aggregate)
    return aggregate


def collect_process_results(result_path, process_items):
    process_results = []
    failed = []
    for item in process_items:
        if not item["result_path"].exists():
            failed.append(item)
            continue
        if item.get("exit_code") not in (0, None):
            print(
                f"[warn] locust process {item['index']} exited {item['exit_code']}; "
                "using written result file",
                flush=True,
            )
        process_results.append(read_json(item["result_path"]))

    if failed:
        indexes = ", ".join(str(item["index"]) for item in failed)
        raise RuntimeError(f"Locust process result missing: {indexes}")

    aggregate = aggregate_locust_results(result_path, process_results)
    aggregate["loadgen_processes"] = len(process_items)
    aggregate["loadgen_process_results"] = [
        {
            "index": item["index"],
            "expected_tcp_client_sockets": item.get("expected_tcp_client_sockets"),
            "expected_target_rps": item.get("expected_target_rps"),
            "spawn_rate": item.get("spawn_rate"),
            "exit_code": item.get("exit_code"),
            "result_path": str(item["result_path"]),
            "log_path": str(item["log_path"]),
        }
        for item in process_items
    ]
    write_json(result_path, aggregate)


def run_locust_single(args, repo_root, server_workers, shards, result_path):
    process_items = []
    run_time = args.warmup_seconds + args.measure_seconds
    process_result_path = result_path.with_name(f"{result_path.stem}_loadgen_1.json")
    log_path = result_path.with_name(f"{result_path.stem}_loadgen_1.log")

    print(
        f"[locust] workers={server_workers} mode=single tcp_sockets={args.locust_users} "
        f"pipeline={args.pipeline_depth} target_rps={args.target_rps:.2f} "
        f"run_time={run_time}s",
        flush=True,
    )

    env = build_locust_env(
        args,
        server_workers,
        shards,
        process_result_path,
        1,
        args.locust_users,
        args.target_rps,
    )
    command = build_locust_single_command(args, args.locust_users, args.spawn_rate)
    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    process_items.append(
        {
            "index": 1,
            "process": process,
            "log_file": log_file,
            "log_path": log_path,
            "result_path": process_result_path,
            "expected_tcp_client_sockets": args.locust_users,
            "expected_target_rps": args.target_rps,
            "spawn_rate": args.spawn_rate,
        }
    )

    try:
        exit_code = process.wait()
        process_items[0]["exit_code"] = exit_code
        log_file.close()
    except BaseException:
        stop_process(process)
        if not log_file.closed:
            log_file.close()
        raise

    collect_process_results(result_path, process_items)
    result = read_json(result_path)
    result["loadgen_mode"] = "single"
    write_json(result_path, result)


def run_locust_distributed(args, repo_root, server_workers, shards, result_path):
    process_count = max(1, args.loadgen_processes)
    expected_splits = split_int(args.locust_users, process_count)
    run_time = args.warmup_seconds + args.measure_seconds
    master_log_path = result_path.with_name(f"{result_path.stem}_loadgen_master.log")
    master_log_file = master_log_path.open("w", encoding="utf-8")
    master_process = None
    process_items = []

    print(
        f"[locust] workers={server_workers} mode=distributed tcp_sockets={args.locust_users} "
        f"loadgen_workers={process_count} pipeline={args.pipeline_depth} "
        f"target_rps={args.target_rps:.2f} run_time={run_time}s",
        flush=True,
    )

    try:
        master_process = subprocess.Popen(
            build_locust_master_command(args),
            cwd=repo_root,
            env=os.environ.copy(),
            stdout=master_log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        time.sleep(0.5)

        for process_index in range(process_count):
            process_result_path = result_path.with_name(
                f"{result_path.stem}_loadgen_{process_index + 1}.json"
            )
            log_path = result_path.with_name(
                f"{result_path.stem}_loadgen_{process_index + 1}.log"
            )
            env = build_locust_env(
                args,
                server_workers,
                shards,
                process_result_path,
                process_index + 1,
                args.locust_users,
                args.target_rps,
            )
            log_file = log_path.open("w", encoding="utf-8")
            process = subprocess.Popen(
                build_locust_worker_command(args),
                cwd=repo_root,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            expected_sockets = expected_splits[process_index] if process_index < len(expected_splits) else 0
            expected_target = (
                args.target_rps * (float(expected_sockets) / float(args.locust_users))
                if args.target_rps > 0.0 and args.locust_users > 0
                else 0.0
            )
            process_items.append(
                {
                    "index": process_index + 1,
                    "process": process,
                    "log_file": log_file,
                    "log_path": log_path,
                    "result_path": process_result_path,
                    "expected_tcp_client_sockets": expected_sockets,
                    "expected_target_rps": expected_target,
                    "spawn_rate": None,
                }
            )

        master_exit_code = master_process.wait()
        if master_exit_code != 0:
            print(f"[warn] locust master exited {master_exit_code}", flush=True)

        for item in process_items:
            try:
                item["exit_code"] = item["process"].wait(timeout=args.stop_timeout_seconds + 10)
            except subprocess.TimeoutExpired:
                stop_process(item["process"])
                item["exit_code"] = item["process"].returncode
            item["log_file"].close()
    except BaseException:
        stop_process(master_process)
        for item in process_items:
            stop_process(item.get("process"))
            if not item["log_file"].closed:
                item["log_file"].close()
        raise
    finally:
        master_log_file.close()

    collect_process_results(result_path, process_items)
    result = read_json(result_path)
    result["loadgen_mode"] = "distributed"
    result["loadgen_master_log"] = str(master_log_path)
    write_json(result_path, result)


def run_locust(args, repo_root, server_workers, shards, result_path):
    if args.locust_users <= 0:
        raise RuntimeError("locust-users must be greater than 0")
    if args.loadgen_processes <= 1:
        run_locust_single(args, repo_root, server_workers, shards, result_path)
        return
    run_locust_distributed(args, repo_root, server_workers, shards, result_path)


def run_one_worker_count(args, repo_root, server_workers):
    result_path = args.artifacts_dir / f"worker_{server_workers}.json"
    server = None
    log_file = None
    started = time.time()
    try:
        server, log_file, shards, log_path = start_tcp_server(args, repo_root, server_workers)
        run_locust(args, repo_root, server_workers, shards, result_path)
        if args.cooldown_seconds > 0:
            print(f"[cooldown] workers={server_workers} seconds={args.cooldown_seconds}", flush=True)
            time.sleep(args.cooldown_seconds)
        result = read_json(result_path)
        target_achievement_ratio = (
            result["average_rps"] / args.target_rps
            if args.target_rps > 0.0
            else None
        )
        result.update(
            {
                "server_workers": server_workers,
                "server_shards": shards,
                "queue_capacity": args.queue_capacity,
                "planner_cache": args.planner_cache,
                "locust_users": args.locust_users,
                "tcp_client_sockets": args.locust_users,
                "loadgen_processes": args.loadgen_processes,
                "locust_master_port": args.locust_master_port,
                "spawn_rate": args.spawn_rate,
                "pipeline_depth": args.pipeline_depth,
                "target_rps": args.target_rps,
                "target_achievement_ratio": target_achievement_ratio,
                "op": args.op,
                "sql": args.sql if args.op == "sql" else None,
                "sql_template": args.sql_template if args.op == "sql" and args.sql_template else None,
                "sql_id_min": args.sql_id_min if args.op == "sql" and args.sql_template else None,
                "sql_id_max": args.sql_id_max if args.op == "sql" and args.sql_template else None,
                "sql_variant_count": args.sql_variant_count if args.op == "sql" and args.sql_template else None,
                "expected_shard_count": shards if args.op == "sql" else 0,
                "cooldown_seconds": args.cooldown_seconds,
                "wall_elapsed_seconds": time.time() - started,
                "server_log": str(log_path),
                "result_path": str(result_path),
            }
        )
        write_json(result_path, result)
        print(
            f"[result] workers={server_workers} avg_rps={result['average_rps']:.2f} "
            f"p95_rps={result['percentiles_rps']['p95']:.2f} "
            f"target_ratio={target_achievement_ratio if target_achievement_ratio is not None else 0.0:.3f} "
            f"failures={result['measure_failures']}",
            flush=True,
        )
        return result
    finally:
        stop_process(server)
        if log_file is not None:
            log_file.close()


def write_markdown_report(path, report):
    lines = [
        "# Locust TCP Worker Sweep",
        "",
        "| workers | shards | active shards | max shard % | tcp sockets | loadgen proc | avg rps | p5 | p15 | p50 | p75 | p95 | failures |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if report["config"].get("target_rps", 0.0) > 0.0:
        lines = [
            "# Locust TCP Worker Sweep",
            "",
            f"- target_rps: `{report['config']['target_rps']:.2f}`",
            "",
            "| workers | shards | active shards | max shard % | tcp sockets | loadgen proc | avg rps | target % | p5 | p15 | p50 | p75 | p95 | failures |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    for run in report["runs"]:
        p = run["percentiles_rps"]
        active_shards, expected_shards, max_shard_percent = shard_summary(run)
        if report["config"].get("target_rps", 0.0) > 0.0:
            target_percent = (run.get("target_achievement_ratio") or 0.0) * 100.0
            lines.append(
                f"| {run['server_workers']} | {run['server_shards']} | "
                f"{active_shards}/{expected_shards} | {max_shard_percent:.2f}% | "
                f"{run.get('tcp_client_sockets', run.get('locust_users', 0))} | "
                f"{run.get('loadgen_processes', 1)} | {run['average_rps']:.2f} | {target_percent:.2f}% | "
                f"{p['p5']:.2f} | {p['p15']:.2f} | {p['p50']:.2f} | "
                f"{p['p75']:.2f} | {p['p95']:.2f} | {run['measure_failures']} |"
            )
        else:
            lines.append(
                f"| {run['server_workers']} | {run['server_shards']} | "
                f"{active_shards}/{expected_shards} | {max_shard_percent:.2f}% | "
                f"{run.get('tcp_client_sockets', run.get('locust_users', 0))} | "
                f"{run.get('loadgen_processes', 1)} | {run['average_rps']:.2f} | "
                f"{p['p5']:.2f} | {p['p15']:.2f} | "
                f"{p['p50']:.2f} | {p['p75']:.2f} | {p['p95']:.2f} | "
                f"{run['measure_failures']} |"
            )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def maybe_plot(args, repo_root, report_path):
    if args.no_plot:
        return None
    plot_path = args.artifacts_dir / "throughput_percentiles.png"
    project_dir = Path(__file__).resolve().parent
    command = [
        sys.executable,
        str(project_dir / "plot_locust_tcp_worker_sweep.py"),
        "--input",
        str(report_path),
        "--output",
        str(plot_path),
    ]
    run_command(command, repo_root)
    return plot_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Locust TCP max-throughput sweep across SQLprocessor server worker counts."
    )
    parser.add_argument("--worker-counts", default="1,2,4,8,12,16,24,32")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15432)
    parser.add_argument("--server-bin", type=Path, default=Path("./tcp_sql_server"))
    parser.add_argument("--shards", type=int, help="default: same as --workers for each run")
    parser.add_argument("--queue-capacity", type=int, default=2048)
    parser.add_argument("--planner-cache", type=int, default=4096)
    parser.add_argument(
        "--locust-users",
        type=int,
        default=20,
        help="total TCP client sockets across all loadgen processes",
    )
    parser.add_argument(
        "--loadgen-processes",
        type=int,
        default=1,
        help="local Locust worker process count; values >1 run Locust distributed mode",
    )
    parser.add_argument("--locust-master-port", type=int, default=5557)
    parser.add_argument("--spawn-rate", type=float, default=20.0)
    parser.add_argument("--pipeline-depth", type=int, default=32)
    parser.add_argument(
        "--target-rps",
        type=float,
        default=50000.0,
        help="0 means push as fast as possible; otherwise pace all users to this total RPS",
    )
    parser.add_argument("--op", choices=["ping", "sql"], default="sql")
    parser.add_argument("--sql", default="SELECT * FROM case_basic_users WHERE id = 2;")
    parser.add_argument(
        "--sql-template",
        default="",
        help=(
            "optional SQL format string for per-request SQL generation; "
            "supports {id}, {seq}, {variant}, and {pad}"
        ),
    )
    parser.add_argument("--sql-id-min", type=int, default=1)
    parser.add_argument("--sql-id-max", type=int, default=1)
    parser.add_argument(
        "--sql-variant-count",
        type=int,
        default=0,
        help="if >0, cycles {variant} and {pad} to vary raw SQL text without changing {id}",
    )
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--measure-seconds", type=int, default=60)
    parser.add_argument("--cooldown-seconds", type=int, default=30)
    parser.add_argument("--startup-timeout-seconds", type=int, default=30)
    parser.add_argument("--stop-timeout-seconds", type=int, default=10)
    parser.add_argument("--artifacts-dir", type=Path, default=Path("artifacts/locust_tcp_worker_sweep"))
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-plot", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    args.artifacts_dir = (repo_root / args.artifacts_dir).resolve()
    args.server_bin = (repo_root / args.server_bin).resolve()
    if args.sql_id_max < args.sql_id_min:
        raise RuntimeError("--sql-id-max must be >= --sql-id-min")
    if args.sql_variant_count < 0:
        raise RuntimeError("--sql-variant-count must be >= 0")
    if args.sql_template:
        try:
            args.sql_template.format(id=args.sql_id_min, seq=0, variant=0, pad="")
        except Exception as exc:
            raise RuntimeError(f"invalid --sql-template: {exc}") from exc
    worker_counts = parse_worker_counts(args.worker_counts)

    if not args.skip_build:
        run_command(["make", "tcp-server"], repo_root)

    report = {
        "config": {
            "worker_counts": worker_counts,
            "host": args.host,
            "port": args.port,
            "queue_capacity": args.queue_capacity,
            "planner_cache": args.planner_cache,
            "locust_users": args.locust_users,
            "tcp_client_sockets": args.locust_users,
            "loadgen_processes": args.loadgen_processes,
            "locust_master_port": args.locust_master_port,
            "spawn_rate": args.spawn_rate,
            "pipeline_depth": args.pipeline_depth,
            "target_rps": args.target_rps,
            "op": args.op,
            "sql": args.sql if args.op == "sql" else None,
            "sql_template": args.sql_template if args.op == "sql" and args.sql_template else None,
            "sql_id_min": args.sql_id_min if args.op == "sql" and args.sql_template else None,
            "sql_id_max": args.sql_id_max if args.op == "sql" and args.sql_template else None,
            "sql_variant_count": args.sql_variant_count if args.op == "sql" and args.sql_template else None,
            "warmup_seconds": args.warmup_seconds,
            "measure_seconds": args.measure_seconds,
            "cooldown_seconds": args.cooldown_seconds,
        },
        "runs": [],
    }

    interrupted = False

    def handle_signal(signum, frame):
        raise KeyboardInterrupt

    previous_sigint = signal.signal(signal.SIGINT, handle_signal)
    previous_sigterm = signal.signal(signal.SIGTERM, handle_signal)
    try:
        for server_workers in worker_counts:
            report["runs"].append(run_one_worker_count(args, repo_root, server_workers))
            report_path = args.artifacts_dir / "report.json"
            write_json(report_path, report)
            write_markdown_report(args.artifacts_dir / "report.md", report)
    except KeyboardInterrupt:
        interrupted = True
        print("[interrupt] writing partial report", flush=True)
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)

    report_path = args.artifacts_dir / "report.json"
    write_json(report_path, report)
    write_markdown_report(args.artifacts_dir / "report.md", report)
    plot_path = maybe_plot(args, repo_root, report_path)
    if plot_path:
        report["plot_path"] = str(plot_path)
        write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 130 if interrupted else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
