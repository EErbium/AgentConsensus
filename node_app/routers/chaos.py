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

    if cfg.mode != FAULT_BYZANTINE:
        return {
            "status": "ok",
            "mode": cfg.mode,
            "active_peers": state.peers,
            "dropped_peers": state._byzantine_drop,
        }

    # Handle Byzantine mode partitioning logic cleanly
    original_cluster = state._original_peers
    if cfg.byzantine_targets:
        targets = [p for p in cfg.byzantine_targets if p in original_cluster]
        state.peers = [p for p in original_cluster if p not in targets]
        state._byzantine_drop = targets
    else:
        midpoint = len(original_cluster) // 2
        state.peers = original_cluster[:midpoint]
        state._byzantine_drop = original_cluster[midpoint:]

    # Enforce minimum consensus group size requirements (f + 1 nodes minimum)
    total_nodes = len(original_cluster)
    max_faulty = (total_nodes - 1) // 3
    required_min = max_faulty + 1

    if len(state.peers) < required_min:
        shortfall = required_min - len(state.peers)
        state.peers.extend(state._byzantine_drop[:shortfall])
        state._byzantine_drop = state._byzantine_drop[shortfall:]

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

        if any(request.url.path.startswith(prefix) for prefix in EXEMPT_PREFIXES):
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

        # Starlette middleware stream reading work-around to prevent hanging downstream endpoints
        body_bytes = b""
        if fault_mode == FAULT_BYZANTINE and request.method == "POST":
            body_bytes = await request.body()
            async def receive():
                return {"type": "http.request", "body": body_bytes, "more_body": False}
            request._receive = receive

        response = await call_next(request)

        if fault_mode != FAULT_BYZANTINE or request.method != "POST":
            return response

        if response.status_code not in (200, 201, 202):
            return response

        target_victims = getattr(state, "_byzantine_drop", [])
        if not target_victims:
            return response

        try:
            payload_json = json.loads(body_bytes)
            tx_id = payload_json.get("tx_id", "")
            sequence_num = payload_json.get("sequence_number", 0)
            
            asyncio.ensure_future(
                _send_corrupted_broadcast(
                    state, target_victims, request.url.path, tx_id, sequence_num
                )
            )
        except Exception as err:
            logger.warning("Byzantine corruption pipeline failure: %s", err)

        return response


async def _send_corrupted_broadcast(state, targets, request_path, tx_id, sequence_num):
    from node_app.core.mesh import build_signed_message

    if "/pre-prepare" in request_path:
        consensus_phase = "PREPARE"
    elif "/prepare" in request_path:
        consensus_phase = "COMMIT"
    else:
        return

    poisoned_tx_id = hashlib.sha256(f"byzantine_{tx_id}".encode()).hexdigest()

    forged_message = build_signed_message(
        state.private_key_hex,
        poisoned_tx_id,
        sequence_num,
        consensus_phase,
        state.node_id,
    )

    endpoint_path = f"/{consensus_phase.lower()}"
    async with httpx.AsyncClient(timeout=2.0) as http_client:
        post_tasks = [
            http_client.post(f"http://{peer}:8000{endpoint_path}", json=forged_message)
            for peer in targets
        ]
        network_outcomes = await asyncio.gather(*post_tasks, return_exceptions=True)
        for peer, outcome in zip(targets, network_outcomes):
            if isinstance(outcome, Exception):
                logger.debug("Corrupt broadcast transmission to %s dropped: %s", peer, outcome)