import argparse
import json
from pathlib import Path


PERCENTILE_KEYS = ["p5", "p15", "p50", "p75", "p95"]


def load_report(path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args():
    parser = argparse.ArgumentParser(description="Plot Locust TCP worker sweep throughput percentiles.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", default="TCP Throughput by Server Worker Count")
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit("matplotlib is required for plotting: python -m pip install matplotlib") from exc

    report = load_report(args.input)
    runs = sorted(report.get("runs", []), key=lambda item: item["server_workers"])
    if not runs:
        raise SystemExit(f"no runs found in {args.input}")

    workers = [run["server_workers"] for run in runs]
    fig, ax = plt.subplots(figsize=(10, 6))
    for key in PERCENTILE_KEYS:
        values = [run["percentiles_rps"][key] for run in runs]
        ax.plot(workers, values, marker="o", linewidth=2, label=key)

    target_rps = float(report.get("config", {}).get("target_rps") or 0.0)
    if target_rps > 0.0:
        ax.axhline(target_rps, color="black", linestyle="--", linewidth=1.5, label="target")

    ax.set_title(args.title)
    ax.set_xlabel("Server worker count")
    ax.set_ylabel("Throughput, requests/sec")
    ax.grid(True, axis="both", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.legend(title="Per-second RPS percentile")
    ax.set_xticks(workers)
    fig.tight_layout()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160)
    print(f"[plot] wrote {args.output}")


if __name__ == "__main__":
    main()
