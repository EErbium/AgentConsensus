import argparse
import asyncio
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from node_app.core.crypto import compute_tx_id, sign_payload
from node_app.schemas.transaction import ExecutionPayload, TransactionEnvelope

PROJECT_ROOT = Path(__file__).parent.parent
TOPOLOGY_FILE = PROJECT_ROOT / "topology.json"
KEYS_FILE = PROJECT_ROOT / "keys.json"
METRICS_CSV = PROJECT_ROOT / "benchmarks" / "results.csv"

OFFLINE_MODE = "OFFLINE"
BYZANTINE_MODE = "MALICIOUS_BYZANTINE"
HEALTHY_MODE = "NONE"

_current_leader_idx = 0


def load_cluster_keys():
    with open(TOPOLOGY_FILE) as f:
        cluster_map = json.load(f)
    with open(KEYS_FILE) as f:
        secret_keys = json.load(f)
    return cluster_map, secret_keys


def get_node_port(node_name: str) -> int:
    return 8000 + int(node_name.split("_")[1]) - 1


def get_node_endpoint(node_name: str) -> str:
    return f"http://127.0.0.1:{get_node_port(node_name)}"


def generate_tx_payload(proposer: str, sequence_num: int, secret_key: str) -> dict:
    now_ts = int(time.time())
    tx_action = ExecutionPayload(
        action="ALLOCATE_FUNDS",
        target_recipient="alice",
        asset_amount=50.0,
        denomination="USD",
    )
    serialized_payload = json.dumps(
        tx_action.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    tx_id = compute_tx_id(proposer, sequence_num, now_ts, serialized_payload)
    signature = sign_payload(secret_key, tx_id.encode())

    envelope = TransactionEnvelope(
        tx_id=tx_id,
        proposer_node=proposer,
        sequence_number=sequence_num,
        timestamp=now_ts,
        execution_payload=tx_action,
        llm_reasoning_hash="0" * 64,
        signatures={proposer: signature},
    )
    return envelope.model_dump(mode="json")


async def wait_for_cluster_startup(cluster_map: dict, timeout: float = 60.0) -> bool:
    async with httpx.AsyncClient(timeout=2.0) as http_client:
        deadline = time.monotonic() + timeout
        node_names = list(cluster_map["nodes"].keys())
        
        while time.monotonic() < deadline:
            ready_nodes = 0
            for name in node_names:
                try:
                    resp = await http_client.get(f"{get_node_endpoint(name)}/health")
                    if resp.status_code == 200:
                        ready_nodes += 1
                except Exception:
                    pass
                    
            if ready_nodes == len(node_names):
                return True
            await asyncio.sleep(1)
            
    return False


async def apply_adversarial_fault(node_name: str, mode: str, drop_set: list[str] | None = None):
    url = f"{get_node_endpoint(node_name)}/chaos/fault"
    payload = {"mode": mode}
    if drop_set:
        payload["byzantine_targets"] = drop_set
        
    async with httpx.AsyncClient(timeout=2.0) as http_client:
        resp = await http_client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


async def clear_adversarial_fault(node_name: str):
    url = f"{get_node_endpoint(node_name)}/chaos/reset"
    async with httpx.AsyncClient(timeout=2.0) as http_client:
        resp = await http_client.post(url)
        resp.raise_for_status()
        return resp.json()


async def dispatch_pre_prepare(http_client: httpx.AsyncClient, node_url: str, payload: dict) -> tuple[float, int]:
    start_time = time.monotonic()
    try:
        resp = await http_client.post(f"{node_url}/pre-prepare", json=payload)
        return (time.monotonic() - start_time) * 1000, resp.status_code
    except Exception:
        return (time.monotonic() - start_time) * 1000, 500


async def force_node_head_reset(http_client: httpx.AsyncClient, node_url: str, next_seq: int):
    try:
        await http_client.post(f"{node_url}/reset-head", json={"target_sequence": next_seq})
    except Exception:
        pass


async def execute_batch_run(
    cluster_map: dict,
    secret_keys: dict,
    proposer: str,
    base_sequence: int,
    size: int,
    scenario_mode: str,
    offline_nodes: list[str],
) -> list[dict]:
    global _current_leader_idx
    node_names = sorted(cluster_map["nodes"].keys())
    healthy_nodes = [name for name in node_names if name not in offline_nodes]
    batch_records = []

    async with httpx.AsyncClient(timeout=2.0) as http_client:
        for idx in range(size):
            sequence_num = base_sequence + idx
            start_mark = time.monotonic()
            round_latencies = []
            committed_successfully = False

            for _ in range(len(node_names)):
                leader = node_names[_current_leader_idx]
                tx_payload = generate_tx_payload(leader, sequence_num, secret_keys[leader])

                tasks = []
                for name in node_names:
                    if scenario_mode == OFFLINE_MODE and name in offline_nodes:
                        continue
                    tasks.append(dispatch_pre_prepare(http_client, get_node_endpoint(name), tx_payload))

                outcomes = await asyncio.gather(*tasks, return_exceptions=True)

                for outcome in outcomes:
                    if isinstance(outcome, tuple):
                        latency_ms, http_status = outcome
                        round_latencies.append(latency_ms)
                        if http_status in (200, 201, 202):
                            committed_successfully = True
                    else:
                        round_latencies.append(None)

                if committed_successfully:
                    break

                _current_leader_idx = (_current_leader_idx + 1) % len(node_names)

            valid_latencies = [l for l in round_latencies if l is not None]
            avg_preprepare = sum(valid_latencies) / len(valid_latencies) if valid_latencies else 0.0
            total_duration = (time.monotonic() - start_mark) * 1000

            await asyncio.sleep(0.05)

            batch_records.append({
                "tx_id": tx_payload["tx_id"],
                "batch_size": size,
                "tx_index": idx,
                "fault_scenario": f"{scenario_mode}_{len(offline_nodes)}f",
                "latency_ms": round(total_duration, 2),
                "avg_preprepare_latency_ms": round(avg_preprepare, 2),
                "accepted_count": 0,
                "status": "PENDING",
            })

        await asyncio.sleep(0.5)

        stalled_any = False
        for record in batch_records:
            target_seq = base_sequence + record["tx_index"]
            nodes_committed = 0
            
            for name in healthy_nodes:
                try:
                    resp = await http_client.get(f"{get_node_endpoint(name)}/verify-tx/{target_seq}", timeout=0.5)
                    if resp.status_code == 200 and resp.json().get("committed") is True:
                        nodes_committed += 1
                except Exception:
                    pass
                    
            record["accepted_count"] = nodes_committed
            record["status"] = "OK" if nodes_committed >= 3 else "FAIL"
            if record["status"] == "FAIL":
                stalled_any = True

        if stalled_any:
            recovery_target = base_sequence + size
            reset_tasks = [
                force_node_head_reset(http_client, get_node_endpoint(name), recovery_target - 1)
                for name in node_names
            ]
            await asyncio.gather(*reset_tasks, return_exceptions=True)

    return batch_records


async def verify_cluster_convergence(cluster_map: dict, valid_tx_ids: set[str]) -> dict:
    node_names = sorted(cluster_map["nodes"].keys())
    state_snapshots = {}
    
    async with httpx.AsyncClient(timeout=2.0) as http_client:
        for name in node_names:
            try:
                resp = await http_client.get(f"{get_node_endpoint(name)}/ledger")
                if resp.status_code == 200:
                    state_snapshots[name] = {entry["tx_id"] for entry in resp.json()}
                else:
                    state_snapshots[name] = set()
            except Exception:
                state_snapshots[name] = set()

    active_ledgers = [tx_set for tx_set in state_snapshots.values() if tx_set]
    synchronized = all(tx_set == active_ledgers[0] for tx_set in active_ledgers) if active_ledgers else True
    
    return {
        "all_nodes_converged": synchronized,
        "node_tx_counts": {name: len(tx_set) for name, tx_set in state_snapshots.items()},
        "total_committed_across_nodes": {
            name: len(valid_tx_ids & tx_set) for name, tx_set in state_snapshots.items()
        },
    }


def spawn_cluster_infrastructure(node_count: int):
    process_run = subprocess.run(
        [sys.executable, "orchestrate.py", "up", "-n", str(node_count)],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(process_run.stdout)
    if process_run.returncode != 0:
        raise RuntimeError(f"Orchestration setup failed: {process_run.stderr}")


def tear_down_cluster_infrastructure():
    process_run = subprocess.run(
        [sys.executable, "orchestrate.py", "down", "--clean"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(process_run.stdout)
    if process_run.returncode != 0:
        raise RuntimeError(f"Orchestration cleanup failed: {process_run.stderr}")


async def execute_scenario(node_count: int, scenario_mode: str, faulty_count: int, batch_sizes: list[int]):
    print(f"\nEvaluating: n={node_count}, scenario={scenario_mode}, faults={faulty_count}")
    
    tear_down_cluster_infrastructure()
    await asyncio.sleep(1)
    spawn_cluster_infrastructure(node_count)
    await asyncio.sleep(2)

    cluster_map, secret_keys = load_cluster_keys()
    node_names = sorted(cluster_map["nodes"].keys())

    if not await wait_for_cluster_startup(cluster_map):
        raise RuntimeError("Nodes failed health checks during initialization timeframe")

    # Keep node_1 out of faulty lists to preserve primary pipeline where possible
    candidate_faulty = [name for name in node_names if name != node_names[0]]
    assigned_faulty = candidate_faulty[:faulty_count] if faulty_count > 0 else []
    drop_set = None

    if scenario_mode == OFFLINE_MODE and assigned_faulty:
        for name in assigned_faulty:
            await apply_adversarial_fault(name, OFFLINE_MODE)

    elif scenario_mode == BYZANTINE_MODE and assigned_faulty:
        honest_nodes = [name for name in node_names if name not in assigned_faulty]
        drop_set = honest_nodes[len(honest_nodes) // 2:] or honest_nodes[:1]
        for name in assigned_faulty:
            await apply_adversarial_fault(name, BYZANTINE_MODE, drop_set=drop_set)

    current_sequence = 1
    collected_metrics = []

    for size in batch_sizes:
        print(f"  Executing Tx Batch Segment Size -> {size}")
        segment_metrics = await execute_batch_run(
            cluster_map, secret_keys, node_names[0], current_sequence, size, scenario_mode, assigned_faulty
        )
        current_sequence += size

        successful_count = sum(1 for entry in segment_metrics if entry["status"] == "OK")
        valid_latencies = [entry["latency_ms"] for entry in segment_metrics if entry["status"] == "OK"]
        avg_lat = sum(valid_latencies) / len(valid_latencies) if valid_latencies else 0.0
        total_seconds = sum(valid_latencies) / 1000.0 if valid_latencies else 1.0
        calculated_tps = successful_count / total_seconds if total_seconds > 0 else 0.0

        print(f"    Consensus Ok: {successful_count}/{size} | Mean Latency: {avg_lat:.1f}ms | Throughput: {calculated_tps:.1f} TPS")
        collected_metrics.extend(segment_metrics)

    await asyncio.sleep(1)

    successful_tx_ids = {entry["tx_id"] for entry in collected_metrics if entry["status"] == "OK"}
    convergence_report = await verify_cluster_convergence(cluster_map, successful_tx_ids)
    print(f"  Consistency Matrix -> {convergence_report}")

    if scenario_mode != HEALTHY_MODE and assigned_faulty:
        for name in assigned_faulty:
            await clear_adversarial_fault(name)

    tear_down_cluster_infrastructure()

    for entry in collected_metrics:
        entry["fault_scenario"] = f"{scenario_mode}_{faulty_count}f"
        entry["total_nodes"] = node_count
        entry["faulty_count"] = faulty_count

    return collected_metrics


def flush_metrics_to_disk(all_runs: list[dict]):
    fieldnames = [
        "total_nodes", "faulty_count", "fault_scenario",
        "batch_size", "tx_index", "tx_id",
        "latency_ms", "avg_preprepare_latency_ms",
        "accepted_count", "status",
    ]
    with open(METRICS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in all_runs:
            writer.writerow({k: record.get(k, "") for k in fieldnames})


async def main():
    parser = argparse.ArgumentParser(description="AgentConsensus Performance Profiling Tool Suite")
    parser.add_argument("--nodes", type=int, default=4)
    parser.add_argument("--fault-mode", type=str, default=HEALTHY_MODE, choices=[HEALTHY_MODE, OFFLINE_MODE, BYZANTINE_MODE])
    parser.add_argument("--fault-count", type=int, default=0)
    parser.add_argument("--batches", type=str, default="100,500,1000")
    parser.add_argument("--run-matrix", action="store_true")
    args = parser.parse_args()

    batch_sizes = [int(size_str.strip()) for size_str in args.batches.split(",")]
    
    execution_matrix = [(args.nodes, args.fault_mode, args.fault_count)]
    if args.run_matrix:
        execution_matrix = [
            (4, HEALTHY_MODE, 0),
            (4, OFFLINE_MODE, 1),
            (4, BYZANTINE_MODE, 1),
            (4, OFFLINE_MODE, 2),
            (4, BYZANTINE_MODE, 2),
        ]

    aggregated_runs = []
    for count, mode, errors in execution_matrix:
        try:
            run_metrics = await execute_scenario(count, mode, errors, batch_sizes)
            aggregated_runs.extend(run_metrics)
        except Exception as scenario_fault:
            print(f"Pipeline Interrupted during scenario execution: {scenario_fault}")
            tear_down_cluster_infrastructure()

    if not aggregated_runs:
        print("\nBenchmark completed with zero records harvested.")
        return

    flush_metrics_to_disk(aggregated_runs)
    print(f"\nTelemetry successfully flushed to disk -> {METRICS_CSV}")


if __name__ == "__main__":
    asyncio.run(main())