import argparse
import json
import os
import binascii
import sys
import docker
import httpx
import time
from cryptography.hazmat.primitives.asymmetric import ed25519

sys.stdout.reconfigure(encoding='utf-8')

NET_NAME = "agent_consensus_net"
IMG_TAG = "agent-consensus-node"
SUBNET_CIDR = "172.28.0.0/16"
NET_GW = "172.28.0.1"
IP_PREFIX = "172.28.0."
STARTING_IP_OCTET = 10

TOPOLOGY_FILE = os.path.abspath("topology.json")
KEYS_FILE = os.path.abspath("keys.json")


def init_docker():
    try:
        return docker.from_env()
    except Exception as e:
        print(f"Failed to connect to Docker daemon: {e}")
        sys.exit(1)


def sync_topology(node_count: int):
    if os.path.exists(TOPOLOGY_FILE) and os.path.exists(KEYS_FILE):
        with open(TOPOLOGY_FILE) as f:
            cluster_map = json.load(f)
        with open(KEYS_FILE) as f:
            secret_keys = json.load(f)
        
        if len(cluster_map.get("nodes", {})) == node_count and len(secret_keys) == node_count:
            return cluster_map, secret_keys

    print(f"Generating key pairs for {node_count} consensus nodes...")
    public_registry = {}
    secret_registry = {}
    
    for idx in range(1, node_count + 1):
        node_name = f"node_{idx}"
        signing_key = ed25519.Ed25519PrivateKey.generate()
        
        raw_seed = signing_key.private_bytes_raw()
        raw_pub = signing_key.public_key().public_bytes_raw()
        
        secret_registry[node_name] = binascii.hexlify(raw_seed).decode()
        public_registry[node_name] = {"public_key": binascii.hexlify(raw_pub).decode()}

    cluster_map = {"nodes": public_registry}
    
    with open(TOPOLOGY_FILE, "w") as f:
        json.dump(cluster_map, f, indent=2)
    with open(KEYS_FILE, "w") as f:
        json.dump(secret_registry, f, indent=2)

    return cluster_map, secret_registry


def setup_overlay_net(engine):
    try:
        return engine.networks.get(NET_NAME)
    except docker.errors.NotFound:
        pass

    ipam_pool = docker.types.IPAMPool(subnet=SUBNET_CIDR, gateway=NET_GW)
    ipam_cfg = docker.types.IPAMConfig(pool_configs=[ipam_pool])
    return engine.networks.create(NET_NAME, driver="bridge", ipam=ipam_cfg)


def compile_node_image(engine):
    print(f"Building {IMG_TAG} from local context...")
    image, build_logs = engine.images.build(path=".", tag=IMG_TAG, rm=True)
    for log_line in build_logs:
        chunk = log_line.get("stream")
        if chunk:
            print(chunk, end="")
    return image


def deploy_cluster(engine, subnet, node_count, secret_keys):
    active_instances = []
    
    for idx in range(1, node_count + 1):
        node_name = f"node_{idx}"
        
        try:
            stale = engine.containers.get(node_name)
            stale.remove(force=True)
        except docker.errors.NotFound:
            pass

        node_ip = f"{IP_PREFIX}{STARTING_IP_OCTET + idx - 1}"
        peer_identifiers = [f"node_{j}" for j in range(1, node_count + 1) if j != idx]

        # The Docker API requires attaching to custom networks via network configuration mappings 
        # during creation if we want static IPs to stick correctly without race conditions.
        endpoint_config = {
            NET_NAME: docker.types.EndpointConfig(
                ipv4_address=node_ip
            )
        }
        network_mode_param = NET_NAME

        instance = engine.containers.run(
            IMG_TAG,
            name=node_name,
            detach=True,
            environment={
                "NODE_ID": node_name,
                "PEERS": ",".join(peer_identifiers),
                "PRIVATE_KEY_HEX": secret_keys[node_name],
            },
            ports={"8000/tcp": ("0.0.0.0", 8000 + idx - 1)},
            volumes={
                TOPOLOGY_FILE: {
                    "bind": "/app/topology.json",
                    "mode": "ro",
                }
            },
            network=network_mode_param,
            networking_config=docker.types.NetworkingConfig(endpoints=endpoint_config)
        )
        
        instance.reload()
        active_instances.append(instance)

    return active_instances


def verify_cluster_health(instances):
    print("\nVerifying HTTP health endpoints...")
    for instance in instances:
        bindings = instance.attrs["NetworkSettings"]["Ports"]["8000/tcp"]
        if not bindings:
            print(f"  WARNING: No port bindings found for {instance.name}")
            continue
            
        host_port = bindings[0]["HostPort"]
        endpoint = f"http://127.0.0.1:{host_port}/health"
        
        healthy = False
        for _ in range(15):
            try:
                resp = httpx.get(endpoint, timeout=1.0)
                if resp.status_code == 200:
                    print(f"  {instance.name} is alive at {endpoint}")
                    healthy = True
                    break
            except Exception:
                pass
            time.sleep(1)
            
        if not healthy:
            print(f"  CRITICAL: {instance.name} failed to respond to health checks.")


def purge_environment(engine, drop_configs=False):
    for instance in engine.containers.list(all=True):
        if instance.name.startswith("node_"):
            print(f"Stopping and dropping container: {instance.name}")
            instance.remove(force=True)
            
    try:
        subnet = engine.networks.get(NET_NAME)
        subnet.remove()
        print(f"Dropped network: {NET_NAME}")
    except docker.errors.NotFound:
        pass

    if not drop_configs:
        return

    for target_path in (TOPOLOGY_FILE, KEYS_FILE):
        if os.path.exists(target_path):
            os.remove(target_path)
            print(f"Purged: {target_path}")


def main():
    cli = argparse.ArgumentParser(description="Consensus cluster orchestrator.")
    cli.add_argument("action", nargs="?", default="up", choices=["up", "down"])
    cli.add_argument("-n", "--nodes", type=int, default=3, help="Total target consensus nodes")
    cli.add_argument("--clean", action="store_true", help="Wipe configurations on teardown")
    args = cli.parse_args()

    engine = init_docker()

    if args.action == "down":
        purge_environment(engine, drop_configs=args.clean)
        return

    cluster_map, secret_keys = sync_topology(args.nodes)
    subnet = setup_overlay_net(engine)
    compile_node_image(engine)
    
    instances = deploy_cluster(engine, subnet, args.nodes, secret_keys)
    verify_cluster_health(instances)

    print("\n--- Network Matrix ---")
    for instance in instances:
        host_port = instance.attrs["NetworkSettings"]["Ports"]["8000/tcp"][0]["HostPort"]
        print(f"  {instance.name} -> http://127.0.0.1:{host_port} [Internal DNS accessible within {NET_NAME}]")