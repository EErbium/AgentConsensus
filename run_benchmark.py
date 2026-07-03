"""
One-shot benchmark: start cluster, inject Byzantine fault, profile, plot.
"""
import argparse
import asyncio
import subprocess
import sys
import time
from pathlib import Path
import httpx

APP_ROOT = Path(__file__).parent

OFFLINE_MODE = "OFFLINE"
BYZANTINE_MODE = "MALICIOUS_BYZANTINE"


async def execute_sub_task(cmd: list[str], task_desc: str, working_dir: str | None = None) -> str:
    print(f"\n=== {task_desc} ===")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=working_dir or str(APP_ROOT),
    )
    stdout, stderr = await proc.communicate()
    
    output_text = stdout.decode() if stdout else ""
    error_text = stderr.decode() if stderr else ""
    
    if proc.returncode != 0:
        print(output_text)
        print(error_text, file=sys.stderr)
        raise RuntimeError(f"{task_desc} failed with exit code {proc.returncode}")
        
    print(output_text)
    return output_text


async def spin_up_cluster(node_count: int):
    await execute_sub_task(
        [sys.executable, "orchestrate.py", "up", "-n", str(node_count)],
        f"Spinning up {node_count}-node consensus cluster",
    )


async def tear_down_cluster():
    await execute_sub_task(
        [sys.executable, "orchestrate.py", "down", "--clean"],
        "Tearing down active cluster",
    )


async def apply_adversarial_fault(victim_node: str, scenario: str, isolated_peers: list[str] | None):
    node_id_number = int(victim_node.split('_')[1])
    target_port = 8000 + node_id_number - 1
    endpoint = f"http://127.0.0.1:{target_port}/chaos/fault"
    
    payload = {"mode": scenario}
    if isolated_peers:
        payload["byzantine_targets"] = isolated_peers

    async with httpx.AsyncClient(timeout=3.0) as http_client:
        resp = await http_client.post(endpoint, json=payload)
        resp.raise_for_status()
        
        status_report = resp.json()
        print(f"  Adversarial environment injected into {victim_node}: {status_report}")
        return status_report


async def execute_perf_profile(node_count: int, scenario: str, total_faults: int, smoke_test: bool):
    batch_sizes = "20,50" if smoke_test else "100,500,1000"
    args_list = [
        sys.executable, "benchmarks/profiler.py",
        "--nodes", str(node_count),
        "--fault-mode", scenario,
        "--fault-count", str(total_faults),
        "--batches", batch_sizes,
    ]
    await execute_sub_task(args_list, f"Profiling system performance (scenario={scenario}, f={total_faults})")


async def render_metrics():
    await execute_sub_task(
        [sys.executable, "benchmarks/plot_results.py"],
        "Rendering visualization charts",
    )


async def main():
    cli = argparse.ArgumentParser(description="End-to-end Adversarial Benchmark for AgentConsensus")
    cli.add_argument("--nodes", type=int, default=4, help="Total system nodes")
    cli.add_argument("--target", type=str, default="node_2", help="Target node for error simulation")
    cli.add_argument("--fault", type=str, default=BYZANTINE_MODE, choices=[OFFLINE_MODE, BYZANTINE_MODE])
    cli.add_argument("--quick", action="store_true", help="Execute rapid smoke test parameters")
    args = cli.parse_args()

    victim_idx = int(args.target.split("_")[1])
    
    isolated_peers = None
    if args.fault == BYZANTINE_MODE:
        isolated_peers = [
            f"node_{idx}"
            for idx in range(1, args.nodes + 1)
            if idx != victim_idx and idx > args.nodes // 2
        ] or [f"node_{1 if victim_idx != 1 else 2}"]

    try:
        await tear_down_cluster()

        # Hard purge stale layers to force image synchronization
        subprocess.run(
            ["docker", "rmi", "-f", "agent-consensus-node"],
            capture_output=True, text=True,
        )

        await spin_up_cluster(args.nodes)

        print("\n=== Poll Cluster Node Readiness ===")
        async with httpx.AsyncClient(timeout=1.5) as check_client:
            for idx in range(1, args.nodes + 1):
                node_port = 8000 + idx - 1
                health_url = f"http://127.0.0.1:{node_port}/health"
                
                online = False
                for _ in range(25):
                    try:
                        resp = await check_client.get(health_url)
                        if resp.status_code == 200:
                            print(f"  node_{idx} responded on port {node_port}")
                            online = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(1)
                    
                if not online:
                    print(f"  WARNING: node_{idx} failed startup validation within timeout limits")

        print("\n=== Verify Chaos Engine Subsystem ===")
        target_port = 8000 + victim_idx - 1
        chaos_status_url = f"http://127.0.0.1:{target_port}/chaos/status"
        
        chaos_ready = False
        async with httpx.AsyncClient(timeout=2.0) as check_client:
            for attempt in range(10):
                try:
                    resp = await check_client.get(chaos_status_url)
                    print(f"  Chaos subsystem status: {resp.json()}")
                    chaos_ready = True
                    break
                except Exception as connection_error:
                    print(f"  Polling chaos endpoint (attempt {attempt+1}/10)... ({connection_error})")
                    await asyncio.sleep(2)
                    
            if not chaos_ready:
                raise RuntimeError(f"Chaos endpoint unreachable on {args.target} via port {target_port}")

        await apply_adversarial_fault(args.target, args.fault, isolated_peers)
        await execute_perf_profile(args.nodes, args.fault, 1, args.quick)
        await render_metrics()

        print(f"\n{'='*60}")
        print("Adversarial scenario execution finished.")
        print(f"  Metrics CSV:  benchmarks/results.csv")
        print(f"  Output Plots: benchmarks/charts/")
        print(f"{'='*60}")

    except Exception as runtime_fault:
        print(f"\nExecution Aborted: {runtime_fault}")
        sys.exit(1)
        
    finally:
        try:
            await tear_down_cluster()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())