import argparse
import asyncio
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from node_app.core.crypto import compute_tx_id, sign_payload, verify_signature
from node_app.schemas.transaction import ExecutionPayload, TransactionEnvelope

PROJECT_ROOT = Path(__file__).parent.parent
TOPOLOGY_PATH = PROJECT_ROOT / "topology.json"
KEYS_PATH = PROJECT_ROOT / "keys.json"
ORCHESTRATE = PROJECT_ROOT / "orchestrate.py"

RESULTS_CSV = PROJECT_ROOT / "benchmarks" / "results.csv"

FAULT_NONE = "NONE"
FAULT_OFFLINE = "OFFLINE"
FAULT_BYZANTINE = "MALICIOUS_BYZANTINE"

_leader_idx = 0


def load_keys():
    with open(TOPOLOGY_PATH) as f:
        topology = json.load(f)
    with open(KEYS_PATH) as f:
        private_keys = json.load(f)
    return topology, private_keys


def get_port(node_name: str) -> int:
    idx = int(node_name.split("_")[1])
    return 8000 + idx - 1


def get_node_url(node_name: str) -> str:
    return f"http://127.0.0.1:{get_port(node_name)}"


def build_envelope(
    proposer: str,
    seq: int,
    private_key_hex: str,
) -> dict:
    now = int(time.time())
    payload = ExecutionPayload(
        action="ALLOCATE_FUNDS",
        target_recipient="alice",
        asset_amount=50.0,
        denomination="USD",
    )
    canonical = json.dumps(
        payload.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    tx_id = compute_tx_id(proposer, seq, now, canonical)

    sig = sign_payload(private_key_hex, tx_id.encode())

    envelope = TransactionEnvelope(
        tx_id=tx_id,
        proposer_node=proposer,
        sequence_number=seq,
        timestamp=now,
        execution_payload=payload,
        llm_reasoning_hash="0" * 64,
        signatures={proposer: sig},
    )
    return envelope.model_dump(mode="json")


async def wait_for_cluster(topology: dict, timeout: float = 30.0) -> bool:
    async with httpx.AsyncClient(timeout=2.0) as client:
        deadline = time.monotonic() + timeout
        nodes = list(topology["nodes"].keys())
        while time.monotonic() < deadline:
            ready = 0
            for name in nodes:
                try:
                    r = await client.get(f"{get_node_url(name)}/health")
                    if r.status_code == 200:
                        ready += 1
                except Exception:
                    pass
            if ready == len(nodes):
                return True
            await asyncio.sleep(1)
    return False


async def inject_fault(
    target_node: str,
    mode: str,
    byzantine_targets: list[str] | None = None,
):
    url = f"{get_node_url(target_node)}/chaos/fault"
    payload: dict = {"mode": mode}
    if byzantine_targets:
        payload["byzantine_targets"] = byzantine_targets
    async with httpx.AsyncClient(timeout=2.0) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        return r.json()


async def clear_fault(target_node: str):
    url = f"{get_node_url(target_node)}/chaos/reset"
    async with httpx.AsyncClient(timeout=2.0) as client:
        r = await client.post(url)
        r.raise_for_status()
        return r.json()


async def send_pre_prepare(
    client: httpx.AsyncClient,
    node_url: str,
    envelope: dict,
) -> tuple[float, int]:
    t0 = time.monotonic()
    r = await client.post(f"{node_url}/pre-prepare", json=envelope)
    t1 = time.monotonic()
    return (t1 - t0) * 1000, r.status_code


async def reset_node_head(client: httpx.AsyncClient, node_url: str, target_seq: int):
    try:
        await client.post(
            f"{node_url}/reset-head",
            json={"target_sequence": target_seq},
        )
    except Exception:
        pass


async def check_ledger(
    client: httpx.AsyncClient,
    node_url: str,
    expected_tx_id: str,
) -> bool:
    try:
        r = await client.get(f"{node_url}/ledger", timeout=3.0)
        if r.status_code != 200:
            return False
        ledger = r.json()
        return any(entry["tx_id"] == expected_tx_id for entry in ledger)
    except Exception:
        return False


async def run_batch(
    topology: dict,
    private_keys: dict,
    proposer: str,
    start_seq: int,
    batch_size: int,
    fault_mode: str,
    faulty_nodes: list[str],
    byzantine_targets: list[str] | None,
) -> list[dict]:
    global _leader_idx
    nodes = sorted(topology["nodes"].keys())
    honest_nodes = [n for n in nodes if n not in faulty_nodes]
    results = []

    async with httpx.AsyncClient(timeout=2.0) as client:
        for i in range(batch_size):
            seq = start_seq + i
            t0 = time.monotonic()
            all_latencies = []

            tx_accepted = False
            for attempt in range(len(nodes)):
                leader = nodes[_leader_idx]
                envelope = build_envelope(leader, seq, private_keys[leader])

                tasks = []
                for name in nodes:
                    if fault_mode == FAULT_OFFLINE and name in faulty_nodes:
                        continue
                    tasks.append(
                        send_pre_prepare(client, get_node_url(name), envelope)
                    )

                outcomes = await asyncio.gather(*tasks, return_exceptions=True)

                for outcome in outcomes:
                    if isinstance(outcome, tuple):
                        lat, code = outcome
                        all_latencies.append(lat)
                        if code in (200, 201, 202):
                            tx_accepted = True
                    elif isinstance(outcome, Exception):
                        all_latencies.append(None)

                if tx_accepted:
                    break

                _leader_idx = (_leader_idx + 1) % len(nodes)

            valid_lats = [l for l in all_latencies if l is not None]
            avg_latency = sum(valid_lats) / len(valid_lats) if valid_lats else 0.0

            t1 = time.monotonic()
            total_latency = (t1 - t0) * 1000

            await asyncio.sleep(0.05)

            results.append({
                "tx_id": envelope["tx_id"],
                "batch_size": batch_size,
                "tx_index": i,
                "fault_scenario": f"{fault_mode}_{len(faulty_nodes)}f",
                "latency_ms": round(total_latency, 2),
                "avg_preprepare_latency_ms": round(avg_latency, 2),
                "accepted_count": 0,
                "status": "PENDING",
            })

        # Wait briefly for consensus to propagate
        await asyncio.sleep(0.5)

        # Fast-path: query in-memory tracker state (no disk I/O)
        any_failed = False
        for record in results:
            seq = start_seq + record["tx_index"]
            accepted_count = 0
            for name in honest_nodes:
                try:
                    r = await client.get(
                        f"{get_node_url(name)}/verify-tx/{seq}",
                        timeout=0.5,
                    )
                    if r.status_code == 200 and r.json().get("committed") == True:
                        accepted_count += 1
                except Exception:
                    pass
            record["accepted_count"] = accepted_count
            record["status"] = "OK" if accepted_count >= 3 else "FAIL"
            if record["status"] == "FAIL":
                any_failed = True

        # Reset head on all nodes if any transaction stalled
        if any_failed:
            next_seq = start_seq + batch_size
            reset_tasks = [
                reset_node_head(client, get_node_url(name), next_seq - 1)
                for name in nodes
            ]
            await asyncio.gather(*reset_tasks, return_exceptions=True)

    return results


async def verify_state_convergence(
    topology: dict,
    expected_tx_ids: set[str],
) -> dict:
    nodes = sorted(topology["nodes"].keys())
    node_ledgers: dict[str, set[str]] = {}
    async with httpx.AsyncClient(timeout=2.0) as client:
        for name in nodes:
            try:
                r = await client.get(f"{get_node_url(name)}/ledger")
                if r.status_code == 200:
                    ledger = r.json()
                    node_ledgers[name] = {e["tx_id"] for e in ledger}
                else:
                    node_ledgers[name] = set()
            except Exception:
                node_ledgers[name] = set()

    committed = [v for v in node_ledgers.values() if v]
    all_same = all(v == committed[0] for v in committed) if committed else True
    return {
        "all_nodes_converged": all_same,
        "node_tx_counts": {n: len(tx) for n, tx in node_ledgers.items()},
        "total_committed_across_nodes": {
            n: len(expected_tx_ids & tx) for n, tx in node_ledgers.items()
        },
    }


async def orchestrate_up(n: int):
    result = subprocess.run(
        [sys.executable, str(ORCHESTRATE), "up", "-n", str(n)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"orchestrate.py up failed: {result.stderr}")


async def orchestrate_down(clean: bool = True):
    cmd = [sys.executable, str(ORCHESTRATE), "down"]
    if clean:
        cmd.append("--clean")
    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"orchestrate.py down failed: {result.stderr}")


async def run_scenario(
    total_nodes: int,
    fault_mode: str,
    faulty_count: int,
    batch_sizes: list[int],
):
    print(f"\n{'='*60}")
    print(f"SCENARIO: n={total_nodes}, fault={fault_mode}, f_count={faulty_count}")
    print(f"{'='*60}")

    await orchestrate_down(clean=True)
    await asyncio.sleep(2)
    await orchestrate_up(total_nodes)
    await asyncio.sleep(3)

    topology, private_keys = load_keys()
    nodes = sorted(topology["nodes"].keys())

    if not await wait_for_cluster(topology, timeout=60):
        raise RuntimeError("Cluster did not become ready in time")

    print(f"Cluster ready: {len(nodes)} nodes")

    # Never assign the fault flag to the primary leader (node_1)
    backup_nodes = [n for n in nodes if n != nodes[0]]
    faulty_nodes = backup_nodes[:faulty_count] if faulty_count > 0 else []
    byzantine_targets = None

    if fault_mode == FAULT_OFFLINE and faulty_nodes:
        print(f"Injecting OFFLINE fault on: {faulty_nodes}")
        for fn in faulty_nodes:
            await inject_fault(fn, FAULT_OFFLINE)

    elif fault_mode == FAULT_BYZANTINE and faulty_nodes:
        print(f"Injecting MALICIOUS_BYZANTINE fault on: {faulty_nodes}")
        honest_nodes = [n for n in nodes if n not in faulty_nodes]
        byzantine_targets = honest_nodes[len(honest_nodes) // 2:] or honest_nodes[:1]
        for fn in faulty_nodes:
            await inject_fault(
                fn,
                FAULT_BYZANTINE,
                byzantine_targets=byzantine_targets,
            )

    if fault_mode != FAULT_NONE and faulty_nodes:
        print(f"   Byzantine targets (drop set): {byzantine_targets}")

    proposer = nodes[0]
    seq = 1
    all_results = []

    for batch_size in batch_sizes:
        print(f"\n  --- Batch size: {batch_size} ---")
        batch_results = await run_batch(
            topology=topology,
            private_keys=private_keys,
            proposer=proposer,
            start_seq=seq,
            batch_size=batch_size,
            fault_mode=fault_mode,
            faulty_nodes=faulty_nodes,
            byzantine_targets=byzantine_targets,
        )
        seq += batch_size

        ok_count = sum(1 for r in batch_results if r["status"] == "OK")
        latencies = [r["latency_ms"] for r in batch_results if r["status"] == "OK"]
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        total_time = sum(latencies) / 1000 if latencies else 1
        tps = ok_count / total_time if total_time > 0 else 0

        print(f"     OK: {ok_count}/{batch_size}, "
              f"Avg Latency: {avg_lat:.1f}ms, TPS: {tps:.1f}")
        all_results.extend(batch_results)

    await asyncio.sleep(2)

    fault_scenario = f"{fault_mode}_{faulty_count}f"
    convergence = await verify_state_convergence(
        topology,
        {r["tx_id"] for r in all_results if r["status"] == "OK"},
    )
    print(f"\n  State Convergence: {convergence}")

    if fault_mode != FAULT_NONE and faulty_nodes:
        print(f"  Clearing faults on faulty nodes...")
        for fn in faulty_nodes:
            await clear_fault(fn)

    await orchestrate_down(clean=True)

    for r in all_results:
        r["fault_scenario"] = fault_scenario
        r["total_nodes"] = total_nodes
        r["faulty_count"] = faulty_count

    return all_results, convergence


async def main():
    parser = argparse.ArgumentParser(
        description="AgentConsensus Adversarial Benchmark Profiler"
    )
    parser.add_argument(
        "--nodes", type=int, default=4,
        help="Total nodes in the cluster (default: 4)"
    )
    parser.add_argument(
        "--fault-mode", type=str, default=FAULT_NONE,
        choices=[FAULT_NONE, FAULT_OFFLINE, FAULT_BYZANTINE],
        help="Fault mode to inject"
    )
    parser.add_argument(
        "--fault-count", type=int, default=0,
        help="Number of faulty nodes (default: 0)"
    )
    parser.add_argument(
        "--batches", type=str, default="100,500,1000",
        help="Comma-separated batch sizes (default: 100,500,1000)"
    )
    parser.add_argument(
        "--run-matrix", action="store_true",
        help="Run the full 3-run test matrix instead of a single scenario"
    )
    args = parser.parse_args()

    batch_sizes = [int(b.strip()) for b in args.batches.split(",")]

    if args.run_matrix:
        matrix = [
            (4, FAULT_NONE, 0),
            (4, FAULT_OFFLINE, 1),
            (4, FAULT_BYZANTINE, 1),
            (4, FAULT_OFFLINE, 2),
            (4, FAULT_BYZANTINE, 2),
        ]
    else:
        matrix = [(args.nodes, args.fault_mode, args.fault_count)]

    all_scenario_results = []

    for n, fault, fcount in matrix:
        try:
            scenario_results, convergence = await run_scenario(
                total_nodes=n,
                fault_mode=fault,
                faulty_count=fcount,
                batch_sizes=batch_sizes,
            )
            all_scenario_results.extend(scenario_results)
        except Exception as e:
            print(f"Scenario FAILED: {e}")
            try:
                await orchestrate_down(clean=True)
            except Exception:
                pass

    if all_scenario_results:
        write_csv(all_scenario_results)
        print(f"\nResults written to {RESULTS_CSV}")
    else:
        print("\nNo results collected.")


def write_csv(results: list[dict]):
    fieldnames = [
        "total_nodes", "faulty_count", "fault_scenario",
        "batch_size", "tx_index", "tx_id",
        "latency_ms", "avg_preprepare_latency_ms",
        "accepted_count", "status",
    ]
    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    print(f"Wrote {len(results)} rows to {RESULTS_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
