import asyncio
import httpx
from node_app.core.crypto import sign_payload

PHASE_PREPARE = "PREPARE"
PHASE_COMMIT = "COMMIT"


def build_signed_message(
    private_key_hex: str,
    tx_id: str,
    sequence_num: int,
    consensus_phase: str,
    validator_identity: str,
) -> dict:
    wire_payload = f"{tx_id}{sequence_num}{consensus_phase}".encode()
    cryptographic_signature = sign_payload(private_key_hex, wire_payload)
    return {
        "tx_id": tx_id,
        "sequence_number": sequence_num,
        "validator": validator_identity,
        "signature": cryptographic_signature,
    }


async def broadcast_to_peers(
    peers: list[str],
    endpoint_path: str,
    serialized_message: dict,
) -> list[Exception | httpx.Response | None]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(2.0)) as http_client:
        delivery_outcomes = []
        for target_node in peers:
            try:
                network_response = await http_client.post(
                    f"http://{target_node}:8000{endpoint_path}", json=serialized_message
                )
                delivery_outcomes.append(network_response)
            except httpx.HTTPError:
                delivery_outcomes.append(None)
            
            # Cooperative yield to prevent event loop starvation during concurrent message bursts
            await asyncio.sleep(0.001)
            
        return delivery_outcomes