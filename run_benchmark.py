"""
One-shot benchmark: start cluster, inject Byzantine fault, profile, plot.

Usage:
    python run_benchmark.py                    # default 4 nodes, Byzantine on node_2
    python run_benchmark.py --nodes 5 --target node_3
    python run_benchmark.py --fault OFFLINE     # crash fault instead of Byzantine
    python run_benchmark.py --quick             # small batch for smoke testing
"""
import argparse
import asyncio
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).parent

FAULT_OFFLINE = "OFFLINE"
FAULT_BYZANTINE = "MALICIOUS_BYZANTINE"


async def run(cmd: list[str], desc: str, cwd: str | None = None) -> str:
    print(f"\n=== {desc} ===")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd or str(BASE),
    )
    stdout, stderr = await proc.communicate()
    out = stdout.decode() if stdout else ""
    err = stderr.decode() if stderr else ""
    if proc.returncode != 0:
        print(out)
        print(err, file=sys.stderr)
        raise RuntimeError(f"{desc} failed (rc={proc.returncode})")
    print(out)
    return out


async def orchestrate_up(nodes: int):
    await run(
        [sys.executable, "orchestrate.py", "up", "-n", str(nodes)],
        f"Starting {nodes}-node cluster",
    )


async def orchestrate_down():
    await run(
        [sys.executable, "orchestrate.py", "down", "--clean"],
        "Tearing down cluster",
    )


async def inject_fault(target: str, mode: str, drop_targets: list[str] | None):
    import httpx

    url = f"http://127.0.0.1:{8000 + int(target.split('_')[1]) - 1}/chaos/fault"
    payload: dict = {"mode": mode}
    if drop_targets:
        payload["byzantine_targets"] = drop_targets

    async with httpx.AsyncClient(timeout=2.0) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        print(f"  Fault injected on {target}: {data}")
        return data


async def run_profiler(nodes: int, fault_mode: str, fault_count: int, quick: bool):
    batches = "20,50" if quick else "100,500,1000"
    cmd = [
        sys.executable, "benchmarks/profiler.py",
        "--nodes", str(nodes),
        "--fault-mode", fault_mode,
        "--fault-count", str(fault_count),
        "--batches", batches,
    ]
    await run(cmd, f"Running profiler (fault={fault_mode}, f={fault_count})")


async def plot_results():
    await run(
        [sys.executable, "benchmarks/plot_results.py"],
        "Generating charts",
    )


async def main():
    parser = argparse.ArgumentParser(
        description="End-to-end Adversarial Benchmark for AgentConsensus"
    )
    parser.add_argument(
        "--nodes", type=int, default=4,
        help="Total nodes (default: 4)"
    )
    parser.add_argument(
        "--target", type=str, default="node_2",
        help="Node to make faulty (default: node_2)"
    )
    parser.add_argument(
        "--fault", type=str, default=FAULT_BYZANTINE,
        choices=[FAULT_OFFLINE, FAULT_BYZANTINE],
        help="Fault type (default: MALICIOUS_BYZANTINE)"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Small batches (20, 50) for a quick smoke test"
    )
    args = parser.parse_args()

    target_idx = int(args.target.split("_")[1])
    target_port = 8000 + target_idx - 1

    if args.fault == FAULT_BYZANTINE:
        drop_peers = [
            f"node_{i}"
            for i in range(1, args.nodes + 1)
            if i != target_idx and i > args.nodes // 2
        ] or [f"node_{1 if target_idx != 1 else 2}"]
    else:
        drop_peers = None

    try:
        await orchestrate_down()

        print("Forcing clean Docker build ...")
        subprocess.run(
            ["docker", "rmi", "-f", "agent-consensus-node"],
            capture_output=True, text=True,
        )

        await orchestrate_up(args.nodes)

        print(f"\n=== Waiting for cluster health ===")
        import httpx
        async with httpx.AsyncClient(timeout=2.0) as client:
            for i in range(1, args.nodes + 1):
                port = 8000 + i - 1
                for attempt in range(20):
                    try:
                        r = await client.get(f"http://127.0.0.1:{port}/health")
                        if r.status_code == 200:
                            print(f"  node_{i} ready (port {port})")
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(1)
                else:
                    print(f"  WARNING: node_{i} not ready after 20s")

        print("\n=== Verifying chaos endpoint on target ===")
        target_port = 8000 + int(args.target.split("_")[1]) - 1
        async with httpx.AsyncClient(timeout=2.0) as client:
            for retry in range(10):
                try:
                    r = await client.get(
                        f"http://127.0.0.1:{target_port}/chaos/status"
                    )
                    print(f"  {r.json()}")
                    break
                except Exception as e:
                    print(f"  retry {retry+1}: waiting ... ({e})")
                    await asyncio.sleep(2)
            else:
                raise RuntimeError(
                    f"Chaos endpoint not available on {args.target} "
                    f"(port {target_port})"
                )

        await inject_fault(args.target, args.fault, drop_peers)

        await run_profiler(args.nodes, args.fault, 1, args.quick)
        await plot_results()

        print(f"\n{'='*60}")
        print("Benchmark complete!")
        print(f"  Results:  benchmarks/results.csv")
        print(f"  Charts:   benchmarks/charts/")
        print(f"{'='*60}")

    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        try:
            await orchestrate_down()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
