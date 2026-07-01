import asyncio
import hashlib
import json
import logging
from typing import Optional
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
import httpx

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chaos", tags=["chaos"])

FAULT_NONE = "NONE"
FAULT_OFFLINE = "OFFLINE"
FAULT_BYZANTINE = "MALICIOUS_BYZANTINE"

EXEMPT_PREFIXES = ("/chaos", "/health", "/docs", "/openapi.json")


class FaultConfig(BaseModel):
    mode: str = FAULT_NONE
    byzantine_targets: Optional[list[str]] = None


@router.get("/status")
async def get_chaos_status(request: Request):
    state = request.app.state
    return {
        "fault_mode": getattr(state, "fault_mode", FAULT_NONE),
        "fault_config": getattr(state, "_fault_config", {}),
        "active_peers": getattr(state, "peers", []),
        "dropped_peers": getattr(state, "_byzantine_drop", []),
    }


@router.post("/fault")
async def set_fault(cfg: FaultConfig, request: Request):
    app = request.app
    state = app.state

    if not hasattr(state, "_original_peers"):
        state._original_peers = list(getattr(state, "peers", []))

    state.peers = list(state._original_peers)
    state._byzantine_drop = []

    if cfg.mode in (FAULT_NONE, ""):
        state.fault_mode = FAULT_NONE
        state._fault_config = {}
        return {"status": "ok", "mode": FAULT_NONE}

    state.fault_mode = cfg.mode
    state._fault_config = cfg.model_dump()

    if cfg.mode == FAULT_BYZANTINE:
        original = state._original_peers
        if cfg.byzantine_targets:
            targets = [p for p in cfg.byzantine_targets if p in original]
            state.peers = [p for p in original if p not in targets]
            state._byzantine_drop = targets
        else:
            mid = len(original) // 2
            state.peers = original[:mid]
            state._byzantine_drop = original[mid:]
        n = len(original)
        f = (n - 1) // 3
        if len(state.peers) < f + 1:
            needed = f + 1 - len(state.peers)
            state.peers.extend(state._byzantine_drop[:needed])
            state._byzantine_drop = state._byzantine_drop[needed:]

    return {
        "status": "ok",
        "mode": cfg.mode,
        "active_peers": state.peers,
        "dropped_peers": state._byzantine_drop,
    }


@router.post("/reset")
async def reset_fault(request: Request):
    state = request.app.state
    if hasattr(state, "_original_peers"):
        state.peers = list(state._original_peers)
    state.fault_mode = FAULT_NONE
    state._fault_config = {}
    state._byzantine_drop = []
    return {"status": "reset"}


class ChaosMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        state = request.app.state
        fault_mode = getattr(state, "fault_mode", FAULT_NONE)

        if any(request.url.path.startswith(p) for p in EXEMPT_PREFIXES):
            return await call_next(request)

        if fault_mode == FAULT_OFFLINE:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": [{
                        "loc": ["server"],
                        "msg": "Node is offline (chaos fault injected)",
                        "type": "chaos.node_offline",
                    }]
                },
            )

        if fault_mode == FAULT_BYZANTINE and request.method == "POST":
            body_bytes = await request.body()
            response = await call_next(request)
            if response.status_code in (200, 201, 202):
                drop_peers = getattr(state, "_byzantine_drop", [])
                if drop_peers:
                    try:
                        data = json.loads(body_bytes)
                        tx_id = data.get("tx_id", "")
                        seq = data.get("sequence_number", 0)
                        path = request.url.path
                        asyncio.ensure_future(
                            _send_corrupted_broadcast(
                                state, drop_peers, path, tx_id, seq
                            )
                        )
                    except Exception as exc:
                        logger.warning("Byzantine corruption failed: %s", exc)
            return response

        return await call_next(request)


async def _send_corrupted_broadcast(state, drop_peers, path, tx_id, seq):
    from node_app.core.mesh import build_signed_message

    if "/pre-prepare" in path:
        phase = "PREPARE"
    elif "/prepare" in path:
        phase = "COMMIT"
    else:
        return

    forged_tx_id = hashlib.sha256(f"byzantine_{tx_id}".encode()).hexdigest()

    payload = build_signed_message(
        state.private_key_hex,
        forged_tx_id,
        seq,
        phase,
        state.node_id,
    )

    endpoint = f"/{phase.lower()}"
    async with httpx.AsyncClient(timeout=5.0) as client:
        tasks = [
            client.post(f"http://{peer}:8000{endpoint}", json=payload)
            for peer in drop_peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for peer, result in zip(drop_peers, results):
            if isinstance(result, Exception):
                logger.debug("Corrupt broadcast to %s failed: %s", peer, result)
