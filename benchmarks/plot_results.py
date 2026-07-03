import argparse
import csv
from pathlib import Path
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
METRICS_CSV = PROJECT_ROOT / "benchmarks" / "results.csv"
CHART_OUT_DIR = PROJECT_ROOT / "benchmarks" / "charts"


def parse_metrics_file(filepath: Path) -> list[dict]:
    with open(filepath, newline="") as f:
        return list(csv.DictReader(f))


def roll_up_benchmarks(records: list[dict]) -> dict:
    grouped_runs = {}
    for entry in records:
        scenario = entry["fault_scenario"]
        batch_size = int(entry["batch_size"])
        run_key = (scenario, batch_size)
        
        if run_key not in grouped_runs:
            grouped_runs[run_key] = {
                "latencies": [], 
                "successful_tx": 0, 
                "total_tx": 0, 
                "faulty_nodes": int(entry["faulty_count"])
            }
            
        grouped_runs[run_key]["latencies"].append(float(entry["latency_ms"]))
        grouped_runs[run_key]["total_tx"] += 1
        if entry["status"] == "OK":
            grouped_runs[run_key]["successful_tx"] += 1

    summary_matrix = {}
    for (scenario, batch_size), metrics in grouped_runs.items():
        durations = metrics["latencies"]
        
        avg_lat = np.mean(durations) if durations else 0.0
        p50 = np.percentile(durations, 50) if durations else 0.0
        p95 = np.percentile(durations, 95) if durations else 0.0
        p99 = np.percentile(durations, 99) if durations else 0.0
        
        total_time_ms = sum(durations) if durations else 1000.0
        tps = (metrics["successful_tx"] / (total_time_ms / 1000.0)) if total_time_ms > 0 else 0.0
        
        summary_matrix[(scenario, batch_size)] = {
            "scenario": scenario,
            "batch": batch_size,
            "faulty_count": metrics["faulty_nodes"],
            "ok": metrics["successful_tx"],
            "total": metrics["total_tx"],
            "avg_latency_ms": round(avg_lat, 1),
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
            "p99_ms": round(p99, 1),
            "throughput_tps": round(tps, 1),
        }
    return summary_matrix


def render_latency_trends(summary_matrix: dict, target_dir: Path):
    target_scenarios = [
        "NONE_0f",
        "OFFLINE_1f",
        "MALICIOUS_BYZANTINE_1f",
        "OFFLINE_2f",
        "MALICIOUS_BYZANTINE_2f",
    ]
    axis_labels = [
        "Baseline\n(0 Faults)",
        "1 Crash\n(Offline)",
        "1 Byzantine\n(Active)",
        "2 Crash\n(Offline)",
        "2 Byzantine\n(Active)",
    ]

    all_batch_sizes = sorted(set(key[1] for key in summary_matrix))
    palette = ["#2196F3", "#FF9800", "#4CAF50"]
    markers = ["o", "s", "^"]

    fig, canvas = plt.subplots(figsize=(10, 6))

    for idx, size in enumerate(all_batch_sizes):
        trend_line = []
        for stage in target_scenarios:
            lookup = (stage, size)
            lat_val = summary_matrix[lookup]["avg_latency_ms"] if lookup in summary_matrix else 0.0
            trend_line.append(lat_val)
            
        canvas.plot(
            range(len(target_scenarios)),
            trend_line,
            label=f"Batch={size}",
            color=palette[idx % len(palette)],
            marker=markers[idx % len(markers)],
            linewidth=2,
            markersize=8,
        )

    canvas.set_xticks(range(len(target_scenarios)))
    canvas.set_xticklabels(axis_labels, fontsize=10)
    canvas.set_xlabel("Fault Scenario", fontsize=12, labelpad=10)
    canvas.set_ylabel("Average Consensus Latency (ms)", fontsize=12, labelpad=10)
    canvas.set_title("Consensus Latency Under Fault Scenarios", fontsize=14, pad=15)
    canvas.legend(title="Transaction Batch Size", fontsize=10, title_fontsize=11)
    canvas.grid(True, linestyle="--", alpha=0.6)
    canvas.set_axisbelow(True)

    for idx, size in enumerate(all_batch_sizes):
        for s_idx, stage in enumerate(target_scenarios):
            lookup = (stage, size)
            if lookup not in summary_matrix:
                continue
            val = summary_matrix[lookup]["avg_latency_ms"]
            if val <= 0:
                continue
            canvas.annotate(
                f"{val:.0f}",
                (s_idx, val),
                textcoords="offset points",
                xytext=(0, 10),
                ha="center",
                fontsize=8,
                color=palette[idx % len(palette)],
            )

    plt.tight_layout()
    fig.savefig(target_dir / "latency_vs_scenario.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def render_throughput_bars(summary_matrix: dict, target_dir: Path):
    target_scenarios = [
        "NONE_0f",
        "OFFLINE_1f",
        "MALICIOUS_BYZANTINE_1f",
        "OFFLINE_2f",
        "MALICIOUS_BYZANTINE_2f",
    ]
    axis_labels = [
        "Baseline\n(0 Faults)",
        "1 Crash\n(Offline)",
        "1 Byzantine\n(Active)",
        "2 Crash\n(Offline)",
        "2 Byzantine\n(Active)",
    ]

    all_batch_sizes = sorted(set(key[1] for key in summary_matrix))
    total_scenarios = len(target_scenarios)
    total_batches = len(all_batch_sizes)

    segment_width = 0.8 / total_batches
    x_coords = np.arange(total_scenarios)
    palette = ["#2196F3", "#FF9800", "#4CAF50"]

    fig, canvas = plt.subplots(figsize=(10, 6))

    for idx, size in enumerate(all_batch_sizes):
        tps_metrics = []
        for stage in target_scenarios:
            lookup = (stage, size)
            tps_val = summary_matrix[lookup]["throughput_tps"] if lookup in summary_matrix else 0.0
            tps_metrics.append(tps_val)
            
        offset = (idx - total_batches / 2 + 0.5) * segment_width
        bars = canvas.bar(
            x_coords + offset,
            tps_metrics,
            segment_width,
            label=f"Batch={size}",
            color=palette[idx % len(palette)],
            edgecolor="white",
            linewidth=0.5,
        )
        
        # Avoid exploding layout with zero labels or flat charts
        max_height = max(tps_metrics) if tps_metrics else 1.0
        for block in bars:
            height = block.get_height()
            if height <= 0:
                continue
            canvas.text(
                block.get_x() + block.get_width() / 2,
                height + (max_height * 0.02),
                f"{height:.0f}",
                ha="center",
                va="bottom",
                fontsize=8,
                rotation=45,
            )

    canvas.set_xticks(x_coords)
    canvas.set_xticklabels(axis_labels, fontsize=10)
    canvas.set_xlabel("Fault Scenario", fontsize=12, labelpad=10)
    canvas.set_ylabel("Throughput (TPS)", fontsize=12, labelpad=10)
    canvas.set_title("Network Throughput Under Fault Scenarios", fontsize=14, pad=15)
    canvas.legend(title="Transaction Batch Size", fontsize=10, title_fontsize=11)
    canvas.grid(True, axis="y", linestyle="--", alpha=0.6)
    canvas.set_axisbelow(True)

    plt.tight_layout()
    fig.savefig(target_dir / "throughput_vs_scenario.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def render_latency_cdf(records: list[dict], target_dir: Path):
    distinct_scenarios = sorted(set(entry["fault_scenario"] for entry in records))
    palette = ["#2196F3", "#FF9800", "#4CAF50", "#F44336", "#9C27B0"]
    styles = ["-", "--", "-.", ":", "-"]

    fig, canvas = plt.subplots(figsize=(10, 6))

    for idx, stage in enumerate(distinct_scenarios):
        raw_latencies = [
            float(entry["latency_ms"])
            for entry in records
            if entry["fault_scenario"] == stage and entry["status"] == "OK"
        ]
        if not raw_latencies:
            continue
            
        raw_latencies.sort()
        total_points = len(raw_latencies)
        probabilities = np.arange(1, total_points + 1) / total_points

        aliases = {
            "NONE_0f": "Baseline (0 Faults)",
            "OFFLINE_1f": "1 Crash (Offline)",
            "MALICIOUS_BYZANTINE_1f": "1 Byzantine",
            "OFFLINE_2f": "2 Crash (Offline)",
            "MALICIOUS_BYZANTINE_2f": "2 Byzantine",
        }
        
        canvas.plot(
            raw_latencies,
            probabilities,
            label=aliases.get(stage, stage),
            color=palette[idx % len(palette)],
            linestyle=styles[idx % len(styles)],
            linewidth=2,
        )

    canvas.set_xlabel("Consensus Latency (ms)", fontsize=12, labelpad=10)
    canvas.set_ylabel("Cumulative Probability", fontsize=12, labelpad=10)
    canvas.set_title("Latency CDF by Fault Scenario", fontsize=14, pad=15)
    canvas.legend(fontsize=10)
    canvas.grid(True, linestyle="--", alpha=0.6)
    canvas.set_axisbelow(True)

    plt.tight_layout()
    fig.savefig(target_dir / "latency_cdf.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot AgentConsensus benchmark results")
    parser.add_argument("--input", type=str, default=str(METRICS_CSV))
    parser.add_argument("--output-dir", type=str, default=str(CHART_OUT_DIR))
    args = parser.parse_args()

    source_csv = Path(args.input)
    target_plots_dir = Path(args.output_dir)
    target_plots_dir.mkdir(parents=True, exist_ok=True)

    if not source_csv.exists():
        print(f"Metrics missing: {source_csv}. Run the performance profiler first.")
        sys.exit(1)

    records = parse_metrics_file(source_csv)
    summary_matrix = roll_up_benchmarks(records)

    render_latency_trends(summary_matrix, target_plots_dir)
    render_throughput_bars(summary_matrix, target_plots_dir)
    render_latency_cdf(records, target_plots_dir)


if __name__ == "__main__":
    main()