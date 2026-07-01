import argparse
import json
import os
import binascii
import sys
import docker

NETWORK_NAME = "agent_consensus_net"
IMAGE_NAME = "agent-consensus-node"
SUBNET = "172.28.0.0/16"
GATEWAY = "172.28.0.1"
BASE_IP = "172.28.0."
START_OCTET = 10
TOPOLOGY_PATH = os.path.abspath("topology.json")
KEYS_PATH = os.path.abspath("keys.json")


def get_client():
    return docker.from_env()


def generate_topology(num_nodes: int):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
    )
    print(f"Generating {num_nodes} Ed25519 key pairs ...")
    nodes_pub: dict[str, dict[str, str]] = {}
    private_keys: dict[str, str] = {}
    for i in range(1, num_nodes + 1):
        name = f"node_{i}"
        pk = Ed25519PrivateKey.generate()
        priv_hex = binascii.hexlify(pk.private_bytes_raw()).decode()
        pub_hex = binascii.hexlify(pk.public_key().public_bytes_raw()).decode()
        nodes_pub[name] = {"public_key": pub_hex}
        private_keys[name] = priv_hex

    topology = {"nodes": nodes_pub}
    with open(TOPOLOGY_PATH, "w") as f:
        json.dump(topology, f, indent=2)
    with open(KEYS_PATH, "w") as f:
        json.dump(private_keys, f, indent=2)

    print(f"  Wrote {TOPOLOGY_PATH}")
    print(f"  Wrote {KEYS_PATH}")
    return topology, private_keys


def load_or_generate_topology(num_nodes: int):
    if os.path.exists(TOPOLOGY_PATH) and os.path.exists(KEYS_PATH):
        with open(TOPOLOGY_PATH) as f:
            topology = json.load(f)
        with open(KEYS_PATH) as f:
            private_keys = json.load(f)
        if (
            len(topology.get("nodes", {})) == num_nodes
            and len(private_keys) == num_nodes
        ):
            print("Reusing existing topology.json and keys.json")
            return topology, private_keys
        print("Node count changed; regenerating topology.")

    return generate_topology(num_nodes)


def ensure_network(client):
    try:
        net = client.networks.get(NETWORK_NAME)
        print(f"Network '{NETWORK_NAME}' already exists, reusing.")
        return net
    except docker.errors.NotFound:
        print(f"Creating bridge network '{NETWORK_NAME}' ...")
        return client.networks.create(
            NETWORK_NAME,
            driver="bridge",
            ipam=docker.types.IPAMConfig(
                pool_configs=[
                    docker.types.IPAMPool(subnet=SUBNET, gateway=GATEWAY)
                ]
            ),
        )


def build_image(client):
    print(f"Building image '{IMAGE_NAME}' ...")
    img, logs = client.images.build(path=".", tag=IMAGE_NAME, rm=True)
    for line in logs:
        stream = line.get("stream")
        if stream:
            print(stream, end="")
    print(f"Image built: {img.short_id}")
    return img


def launch_containers(client, network, count, private_keys):
    containers = []
    for i in range(1, count + 1):
        name = f"node_{i}"
        try:
            old = client.containers.get(name)
            if old.status == "running":
                print(f"  {name} already running, skipping.")
                containers.append(old)
                continue
            print(f"  Removing stale container {name} ...")
            old.remove(force=True)
        except docker.errors.NotFound:
            pass

        ip = f"{BASE_IP}{START_OCTET + i - 1}"
        peers = [
            f"node_{j}" for j in range(1, count + 1) if j != i
        ]

        print(f"  Launching {name} @ {ip} ...")
        c = client.containers.run(
            IMAGE_NAME,
            name=name,
            detach=True,
            remove=True,
            environment={
                "NODE_ID": name,
                "PEERS": ",".join(peers),
                "PRIVATE_KEY_HEX": private_keys[name],
            },
            ports={"8000/tcp": ("0.0.0.0", 8000 + i - 1)},
            volumes={
                TOPOLOGY_PATH: {
                    "bind": "/app/topology.json",
                    "mode": "ro",
                }
            },
        )
        network.connect(c, ipv4_address=ip)
        c.reload()
        containers.append(c)

    return containers


def wait_for_ready(containers):
    import httpx
    import time

    for c in containers:
        port = c.attrs["NetworkSettings"]["Ports"]["8000/tcp"][0]["HostPort"]
        url = f"http://127.0.0.1:{port}/health"
        for attempt in range(15):
            try:
                r = httpx.get(url, timeout=2)
                if r.status_code == 200:
                    print(f"  {c.name} ready at {url}")
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            print(f"  WARNING: {c.name} did not respond in time")


def destroy_all(client, clean_files=False):
    print("Tearing down all agent-consensus containers and network ...")
    for c in client.containers.list(
        filters={"network": NETWORK_NAME}, all=True
    ):
        print(f"  Removing container {c.name} ...")
        c.remove(force=True)
    try:
        net = client.networks.get(NETWORK_NAME)
        net.remove()
        print(f"  Removed network '{NETWORK_NAME}'.")
    except docker.errors.NotFound:
        pass

    if clean_files:
        for p in (TOPOLOGY_PATH, KEYS_PATH):
            if os.path.exists(p):
                os.remove(p)
                print(f"  Removed {p}")

    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Orchestrate AgentConsensus FastAPI nodes on Docker."
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="up",
        choices=["up", "down"],
        help="'up' (default) to spin up, 'down' to tear down",
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=int,
        default=3,
        help="Number of nodes to launch (default: 3)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Also remove topology.json and keys.json on 'down'",
    )
    args = parser.parse_args()

    client = get_client()

    if args.action == "down":
        destroy_all(client, clean_files=args.clean)
        return

    topology, private_keys = load_or_generate_topology(args.nodes)
    network = ensure_network(client)
    build_image(client)
    containers = launch_containers(client, network, args.nodes, private_keys)
    print(f"\n{len(containers)} node(s) launched. Waiting for readiness ...")
    wait_for_ready(containers)

    print("\n--- Summary ---")
    for c in containers:
        port = c.attrs["NetworkSettings"]["Ports"]["8000/tcp"][0]["HostPort"]
        print(f"  {c.name}: http://127.0.0.1:{port}/health")
        print(f"          http://127.0.0.1:{port}/ledger")
        print(f"          POST http://127.0.0.1:{port}/transaction")
    print(f"  Internal DNS: node_1, node_2, ... inside '{NETWORK_NAME}'")
    print("Done.")


if __name__ == "__main__":
    main()
