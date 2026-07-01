import os
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from node_app.schemas.transaction import TransactionEnvelope
from node_app.schemas.ledger_entry import LedgerEntry
from node_app.core.invariants import InvariantViolation, validate_all
from node_app.core.crypto import verify_envelope_signatures
from node_app.core.consensus_tracker import ConsensusTracker
from node_app.routers.consensus import router as consensus_router
from node_app.routers.chaos import router as chaos_router
from node_app.routers.chaos import ChaosMiddleware

NODE_ID = os.environ["NODE_ID"]
PRIVATE_KEY_HEX = os.environ["PRIVATE_KEY_HEX"]

TOPOLOGY_PATH = Path("/app/topology.json")
with open(TOPOLOGY_PATH) as f:
    topology = json.load(f)

PUBLIC_KEYS: dict[str, str] = {
    name: info["public_key"] for name, info in topology["nodes"].items()
}

PEERS = [name for name in PUBLIC_KEYS if name != NODE_ID]

STORAGE_DIR = Path(__file__).parent / "storage"
STORAGE_DIR.mkdir(exist_ok=True)
LEDGER_PATH = STORAGE_DIR / "ledger.json"
if not LEDGER_PATH.exists():
    LEDGER_PATH.write_text("[]", encoding="utf-8")

lock = asyncio.Lock()

app = FastAPI(title=f"AgentConsensus - {NODE_ID}")

app.state.node_id = NODE_ID
app.state.private_key_hex = PRIVATE_KEY_HEX
app.state.public_keys = PUBLIC_KEYS
app.state.peers = PEERS
app.state.tracker = ConsensusTracker(n=len(PUBLIC_KEYS))
app.state.ledger_lock = lock
app.state.ledger_path = LEDGER_PATH
app.state.fault_mode = "NONE"

app.add_middleware(ChaosMiddleware)
app.include_router(consensus_router)
app.include_router(chaos_router)


@app.get("/health")
async def health():
    return {"node": NODE_ID, "status": "ok"}


@app.post("/transaction")
async def submit_transaction(envelope: TransactionEnvelope):
    violations = validate_all(envelope.execution_payload, envelope.timestamp)

    sig_failures = verify_envelope_signatures(envelope, PUBLIC_KEYS)
    if sig_failures:
        violations.append(
            InvariantViolation(
                f"Invalid signatures from: {', '.join(sig_failures)}",
                field_path="signatures",
                error_type="invariant.signature_verification",
            )
        )

    if violations:
        detail = [
            {
                "loc": ["body"] + v.field_path.split("."),
                "msg": v.message,
                "type": v.error_type,
            }
            for v in violations
        ]
        return JSONResponse(status_code=422, content={"detail": detail})

    async with lock:
        ledger: list[dict] = json.loads(LEDGER_PATH.read_text("utf-8"))
        entry = LedgerEntry(
            ledger_index=len(ledger) + 1,
            tx_id=envelope.tx_id,
            status="COMMITTED",
            validated_at=datetime.now(timezone.utc),
            envelope=envelope,
        )
        ledger.append(entry.model_dump(mode="json"))
        LEDGER_PATH.write_text(
            json.dumps(ledger, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    return JSONResponse(
        status_code=201,
        content=entry.model_dump(mode="json"),
    )


@app.get("/ledger")
async def read_ledger():
    async with lock:
        ledger: list[dict] = json.loads(LEDGER_PATH.read_text("utf-8"))
    return ledger
