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
) -> list[Exception | httpx.Response | None]:
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(2.0),
    ) as client:
        results = []
        for peer in peers:
            try:
                r = await client.post(
                    f"http://{peer}:8000{endpoint}", json=payload
                )
                results.append(r)
            except httpx.HTTPError:
                results.append(None)
            # Cooperatively yield to let the event loop process incoming packets
            await asyncio.sleep(0.001)
        return results
