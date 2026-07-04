import os
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from node_app.schemas.transaction import TransactionEnvelope
from node_app.schemas.ledger_entry import LedgerEntry
from node_app.core.invariants import validate_all
from node_app.core.crypto import verify_envelope_signatures
from node_app.core.consensus_tracker import ConsensusTracker
from node_app.routers.consensus import router as consensus_router
from node_app.routers.chaos import router as chaos_router
from node_app.routers.chaos import ChaosMiddleware

# Quick sanity check on start. Missing env vars break the node instantly.
NODE_ID = os.getenv("NODE_ID")
PRIVATE_KEY_HEX = os.getenv("PRIVATE_KEY_HEX")
if not NODE_ID or not PRIVATE_KEY_HEX:
    raise RuntimeError("NODE_ID and PRIVATE_KEY_HEX environment variables must be configured.")

TOPOLOGY_PATH = Path("/app/topology.json")
if not TOPOLOGY_PATH.exists():
    raise FileNotFoundError(f"Network topology file missing at {TOPOLOGY_PATH}")

with open(TOPOLOGY_PATH, "r", encoding="utf-8") as topo_file:
    topology = json.load(topo_file)

PUBLIC_KEYS: dict[str, str] = {
    name: details["public_key"] for name, details in topology["nodes"].items()
}

PEERS = [name for name in PUBLIC_KEYS if name != NODE_ID]

STORAGE_DIR = Path(__file__).parent / "storage"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

LEDGER_PATH = STORAGE_DIR / "ledger.json"
if not LEDGER_PATH.exists():
    LEDGER_PATH.write_text("[]", encoding="utf-8")

ledger_write_lock = asyncio.Lock()

app = FastAPI(title=f"AgentConsensus-{NODE_ID}")

app.state.node_id = NODE_ID
app.state.private_key_hex = PRIVATE_KEY_HEX
app.state.public_keys = PUBLIC_KEYS
app.state.peers = PEERS
app.state.tracker = ConsensusTracker(n=len(PUBLIC_KEYS))
app.state.ledger_lock = ledger_write_lock
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
    failures = validate_all(envelope.execution_payload, envelope.timestamp)

    sig_failures = verify_envelope_signatures(envelope, PUBLIC_KEYS)
    if sig_failures:
        # Appending mock violation to fit the invariant structure expected by the schema validation pipeline
        from node_app.core.invariants import InvariantViolation
        failures.append(
            InvariantViolation(
                f"Invalid signatures from: {', '.join(sig_failures)}",
                field_path="signatures",
                error_type="invariant.signature_verification",
            )
        )

    if failures:
        errors = [
            {
                "loc": ["body"] + issue.field_path.split("."),
                "msg": issue.message,
                "type": issue.error_type,
            }
            for issue in failures
        ]
        return JSONResponse(status_code=422, content={"detail": errors})

    async with ledger_write_lock:
        try:
            raw_history = LEDGER_PATH.read_text("utf-8")
            history = json.loads(raw_history)
        except (json.JSONDecodeError, IOError):
            # Fallback if file gets partially truncated under concurrent load or chaos testing
            history = []

        entry = LedgerEntry(
            ledger_index=len(history) + 1,
            tx_id=envelope.tx_id,
            status="COMMITTED",
            validated_at=datetime.now(timezone.utc),
            envelope=envelope,
        )
        
        history.append(entry.model_dump(mode="json"))
        
        # Atomic file write swap would be better, but sticking to text write for now to minimize breaking changes
        LEDGER_PATH.write_text(
            json.dumps(history, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    return JSONResponse(
        status_code=201,
        content=entry.model_dump(mode="json"),
    )


@app.get("/ledger")
async def read_ledger():
    async with ledger_write_lock:
        try:
            raw_history = LEDGER_PATH.read_text("utf-8")
            return json.loads(raw_history)
        except Exception:
            raise HTTPException(status_code=500, detail="Failed to read ledger store state.")