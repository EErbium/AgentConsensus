import argparse
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
RESULTS_CSV = PROJECT_ROOT / "benchmarks" / "results.csv"
OUTPUT_DIR = PROJECT_ROOT / "benchmarks" / "charts"


def load_results(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def compute_aggregates(rows: list[dict]) -> dict:
    groups: dict[str, dict] = {}
    for r in rows:
        scenario = r["fault_scenario"]
        batch = int(r["batch_size"])
        key = (scenario, batch)
        if key not in groups:
            groups[key] = {"latencies": [], "ok": 0, "total": 0, "faulty_count": int(r["faulty_count"])}
        groups[key]["latencies"].append(float(r["latency_ms"]))
        groups[key]["total"] += 1
        if r["status"] == "OK":
            groups[key]["ok"] += 1

    aggregates = {}
    for (scenario, batch), g in groups.items():
        if g["latencies"]:
            avg_lat = np.mean(g["latencies"])
            p50 = np.percentile(g["latencies"], 50)
            p95 = np.percentile(g["latencies"], 95)
            p99 = np.percentile(g["latencies"], 99)
        else:
            avg_lat = p50 = p95 = p99 = 0.0
        total_time_s = sum(g["latencies"]) / 1000 if g["latencies"] else 1
        tps = g["ok"] / total_time_s if total_time_s > 0 else 0
        aggregates[(scenario, batch)] = {
            "scenario": scenario,
            "batch": batch,
            "faulty_count": g["faulty_count"],
            "ok": g["ok"],
            "total": g["total"],
            "avg_latency_ms": round(avg_lat, 1),
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
            "p99_ms": round(p99, 1),
            "throughput_tps": round(tps, 1),
        }
    return aggregates


def plot_latency_vs_scenario(aggregates: dict, output_dir: Path):
    scenarios_order = [
        "NONE_0f",
        "OFFLINE_1f",
        "MALICIOUS_BYZANTINE_1f",
        "OFFLINE_2f",
        "MALICIOUS_BYZANTINE_2f",
    ]
    scenario_labels = [
        "Baseline\n(0 Faults)",
        "1 Crash\n(Offline)",
        "1 Byzantine\n(Active)",
        "2 Crash\n(Offline)",
        "2 Byzantine\n(Active)",
    ]

    batch_sizes = sorted(set(k[1] for k in aggregates))
    colors = ["#2196F3", "#FF9800", "#4CAF50"]
    markers = ["o", "s", "^"]

    fig, ax = plt.subplots(figsize=(10, 6))

    for bi, bs in enumerate(batch_sizes):
        lats = []
        for sc in scenarios_order:
            key = (sc, bs)
            if key in aggregates:
                lats.append(aggregates[key]["avg_latency_ms"])
            else:
                lats.append(0)
        ax.plot(
            range(len(scenarios_order)),
            lats,
            label=f"Batch={bs}",
            color=colors[bi % len(colors)],
            marker=markers[bi % len(markers)],
            linewidth=2,
            markersize=8,
        )

    ax.set_xticks(range(len(scenarios_order)))
    ax.set_xticklabels(scenario_labels, fontsize=10)
    ax.set_xlabel("Fault Scenario", fontsize=12, labelpad=10)
    ax.set_ylabel("Average Consensus Latency (ms)", fontsize=12, labelpad=10)
    ax.set_title("Consensus Latency Under Fault Scenarios", fontsize=14, pad=15)
    ax.legend(title="Transaction Batch Size", fontsize=10, title_fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)

    for bi, bs in enumerate(batch_sizes):
        for si, sc in enumerate(scenarios_order):
            key = (sc, bs)
            if key in aggregates:
                val = aggregates[key]["avg_latency_ms"]
                if val > 0:
                    ax.annotate(
                        f"{val:.0f}",
                        (si, val),
                        textcoords="offset points",
                        xytext=(0, 10),
                        ha="center",
                        fontsize=8,
                        color=colors[bi % len(colors)],
                    )

    plt.tight_layout()
    path = output_dir / "latency_vs_scenario.pdf"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "latency_vs_scenario.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_throughput_bar_chart(aggregates: dict, output_dir: Path):
    scenarios_order = [
        "NONE_0f",
        "OFFLINE_1f",
        "MALICIOUS_BYZANTINE_1f",
        "OFFLINE_2f",
        "MALICIOUS_BYZANTINE_2f",
    ]
    scenario_labels = [
        "Baseline\n(0 Faults)",
        "1 Crash\n(Offline)",
        "1 Byzantine\n(Active)",
        "2 Crash\n(Offline)",
        "2 Byzantine\n(Active)",
    ]

    batch_sizes = sorted(set(k[1] for k in aggregates))
    n_scenarios = len(scenarios_order)
    n_batches = len(batch_sizes)

    bar_width = 0.8 / n_batches
    x = np.arange(n_scenarios)

    colors = ["#2196F3", "#FF9800", "#4CAF50"]

    fig, ax = plt.subplots(figsize=(10, 6))

    for bi, bs in enumerate(batch_sizes):
        tps_vals = []
        for sc in scenarios_order:
            key = (sc, bs)
            if key in aggregates:
                tps_vals.append(aggregates[key]["throughput_tps"])
            else:
                tps_vals.append(0)
        offset = (bi - n_batches / 2 + 0.5) * bar_width
        bars = ax.bar(
            x + offset,
            tps_vals,
            bar_width,
            label=f"Batch={bs}",
            color=colors[bi % len(colors)],
            edgecolor="white",
            linewidth=0.5,
        )
        for bar, val in zip(bars, tps_vals):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(tps_vals) * 0.02,
                    f"{val:.0f}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    rotation=45,
                )

    ax.set_xticks(x)
    ax.set_xticklabels(scenario_labels, fontsize=10)
    ax.set_xlabel("Fault Scenario", fontsize=12, labelpad=10)
    ax.set_ylabel("Throughput (TPS)", fontsize=12, labelpad=10)
    ax.set_title("Network Throughput Under Fault Scenarios", fontsize=14, pad=15)
    ax.legend(title="Transaction Batch Size", fontsize=10, title_fontsize=11)
    ax.grid(True, axis="y", linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = output_dir / "throughput_vs_scenario.pdf"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "throughput_vs_scenario.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def plot_latency_cdf(rows: list[dict], output_dir: Path):
    scenarios = sorted(set(r["fault_scenario"] for r in rows))
    colors = ["#2196F3", "#FF9800", "#4CAF50", "#F44336", "#9C27B0"]
    linestyles = ["-", "--", "-.", ":", "-"]

    fig, ax = plt.subplots(figsize=(10, 6))

    for si, sc in enumerate(scenarios):
        lats = [
            float(r["latency_ms"])
            for r in rows
            if r["fault_scenario"] == sc and r["status"] == "OK"
        ]
        if not lats:
            continue
        lats.sort()
        n = len(lats)
        y = np.arange(1, n + 1) / n

        label_map = {
            "NONE_0f": "Baseline (0 Faults)",
            "OFFLINE_1f": "1 Crash (Offline)",
            "MALICIOUS_BYZANTINE_1f": "1 Byzantine",
            "OFFLINE_2f": "2 Crash (Offline)",
            "MALICIOUS_BYZANTINE_2f": "2 Byzantine",
        }
        ax.plot(
            lats,
            y,
            label=label_map.get(sc, sc),
            color=colors[si % len(colors)],
            linestyle=linestyles[si % len(linestyles)],
            linewidth=2,
        )

    ax.set_xlabel("Consensus Latency (ms)", fontsize=12, labelpad=10)
    ax.set_ylabel("Cumulative Probability", fontsize=12, labelpad=10)
    ax.set_title("Latency CDF by Fault Scenario", fontsize=14, pad=15)
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = output_dir / "latency_cdf.pdf"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    fig.savefig(output_dir / "latency_cdf.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot AgentConsensus benchmark results"
    )
    parser.add_argument(
        "--input", type=str, default=str(RESULTS_CSV),
        help=f"Path to results CSV (default: {RESULTS_CSV})"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help=f"Output directory for charts (default: {OUTPUT_DIR})"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Error: results file not found: {input_path}")
        print("Run benchmarks/profiler.py first to generate results.")
        sys.exit(1)

    print(f"Loading results from {input_path} ...")
    rows = load_results(str(input_path))
    print(f"  Loaded {len(rows)} rows")

    aggregates = compute_aggregates(rows)
    print(f"  Aggregated {len(aggregates)} scenario/batch groups")

    print("\nGenerating charts ...")
    plot_latency_vs_scenario(aggregates, output_dir)
    plot_throughput_bar_chart(aggregates, output_dir)
    plot_latency_cdf(rows, output_dir)

    print(f"\nAll charts saved to {output_dir}/")


if __name__ == "__main__":
    import sys
    main()
