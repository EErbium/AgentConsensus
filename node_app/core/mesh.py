import asyncio
import httpx

from node_app.core.crypto import sign_payload

PHASE_PREPARE = "PREPARE"
PHASE_COMMIT = "COMMIT"


def build_signed_message(
    private_key_hex: str,
    tx_id: str,
    seq: int,
    phase: str,
    validator: str,
) -> dict:
    msg = f"{tx_id}{seq}{phase}".encode()
    signature = sign_payload(private_key_hex, msg)
    return {
        "tx_id": tx_id,
        "sequence_number": seq,
        "validator": validator,
        "signature": signature,
    }


async def broadcast_to_peers(
    peers: list[str],
    endpoint: str,
    payload: dict,
) -> list[Exception | httpx.Response]:
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(5.0),
    ) as client:
        tasks = [
            client.post(f"http://{peer}:8000{endpoint}", json=payload)
            for peer in peers
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
