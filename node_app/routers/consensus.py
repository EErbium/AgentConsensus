import json
from datetime import datetime, timezone
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from node_app.schemas.transaction import TransactionEnvelope
from node_app.schemas.consensus import PrepareMessage, CommitMessage
from node_app.schemas.ledger_entry import LedgerEntry
from node_app.core.crypto import verify_signature, sign_payload
from node_app.core.mesh import build_signed_message, broadcast_to_peers, PHASE_COMMIT

router = APIRouter()


@router.post("/pre-prepare")
async def pre_prepare(
    envelope: TransactionEnvelope,
    background: BackgroundTasks,
    request: Request,
):
    state = request.app.state

    pub_hex = state.public_keys.get(envelope.proposer_node)
    if pub_hex is None:
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "proposer_node"],
                        "msg": f"Unknown proposer {envelope.proposer_node}",
                        "type": "invariant.unknown_proposer",
                    }
                ]
            },
        )

    msg = envelope.tx_id.encode()
    sig_hex = envelope.signatures.get(envelope.proposer_node, "")
    if not verify_signature(pub_hex, msg, sig_hex):
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "signatures"],
                        "msg": f"Invalid proposer signature for {envelope.proposer_node}",
                        "type": "invariant.proposer_signature",
                    }
                ]
            },
        )

    seq = envelope.sequence_number
    ok = await state.tracker.add_pre_prepare(seq, envelope)
    if not ok:
        return JSONResponse(
            status_code=409,
            content={
                "detail": [
                    {
                        "loc": ["body", "sequence_number"],
                        "msg": (
                            f"Expected sequence {state.tracker.next_sequence()}, "
                            f"got {seq}"
                        ),
                        "type": "invariant.sequence_mismatch",
                    }
                ]
            },
        )

    background.add_task(
        _broadcast_prepares,
        state.peers,
        state.private_key_hex,
        state.node_id,
        envelope.tx_id,
        envelope.sequence_number,
    )

    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "tx_id": envelope.tx_id,
            "sequence_number": envelope.sequence_number,
        },
    )


@router.post("/prepare")
async def prepare(
    msg: PrepareMessage,
    background: BackgroundTasks,
    request: Request,
):
    state = request.app.state

    pub_hex = state.public_keys.get(msg.validator)
    if pub_hex is None:
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "validator"],
                        "msg": f"Unknown validator {msg.validator}",
                        "type": "invariant.unknown_validator",
                    }
                ]
            },
        )

    if not _verify_phase_sig(pub_hex, msg.tx_id, msg.sequence_number, "PREPARE", msg.signature):
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "signature"],
                        "msg": f"Invalid prepare signature from {msg.validator}",
                        "type": "invariant.prepare_signature",
                    }
                ]
            },
        )

    if not await state.tracker.has_pre_prepare(msg.sequence_number):
        return JSONResponse(
            status_code=404,
            content={
                "detail": [
                    {
                        "loc": ["body", "sequence_number"],
                        "msg": (
                            f"No pre-prepare cached for sequence "
                            f"{msg.sequence_number}"
                        ),
                        "type": "invariant.missing_pre_prepare",
                    }
                ]
            },
        )

    threshold_met, _ = await state.tracker.add_prepare_vote(
        msg.sequence_number, msg.validator
    )

    if threshold_met:
        background.add_task(
            _broadcast_commits,
            state.peers,
            state.private_key_hex,
            state.node_id,
            msg.tx_id,
            msg.sequence_number,
        )

    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "tx_id": msg.tx_id,
            "sequence_number": msg.sequence_number,
        },
    )


@router.post("/commit")
async def commit(
    msg: CommitMessage,
    request: Request,
):
    state = request.app.state

    pub_hex = state.public_keys.get(msg.validator)
    if pub_hex is None:
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "validator"],
                        "msg": f"Unknown validator {msg.validator}",
                        "type": "invariant.unknown_validator",
                    }
                ]
            },
        )

    if not _verify_phase_sig(pub_hex, msg.tx_id, msg.sequence_number, "COMMIT", msg.signature):
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {
                        "loc": ["body", "signature"],
                        "msg": f"Invalid commit signature from {msg.validator}",
                        "type": "invariant.commit_signature",
                    }
                ]
            },
        )

    threshold_met, envelope = await state.tracker.add_commit_vote(
        msg.sequence_number, msg.validator
    )

    if threshold_met and envelope is not None:
        async with state.ledger_lock:
            ledger: list[dict] = json.loads(
                state.ledger_path.read_text("utf-8")
            )
            entry = LedgerEntry(
                ledger_index=len(ledger) + 1,
                tx_id=envelope.tx_id,
                status="COMMITTED",
                validated_at=datetime.now(timezone.utc),
                envelope=envelope,
            )
            ledger.append(entry.model_dump(mode="json"))
            state.ledger_path.write_text(
                json.dumps(ledger, indent=2, default=str, ensure_ascii=False),
                encoding="utf-8",
            )

    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "tx_id": msg.tx_id,
            "sequence_number": msg.sequence_number,
        },
    )


def _verify_phase_sig(
    pub_hex: str,
    tx_id: str,
    seq: int,
    phase: str,
    signature: str,
) -> bool:
    return verify_signature(
        pub_hex,
        f"{tx_id}{seq}{phase}".encode(),
        signature,
    )


async def _broadcast_prepares(
    peers: list[str],
    private_key_hex: str,
    node_id: str,
    tx_id: str,
    seq: int,
):
    payload = build_signed_message(
        private_key_hex, tx_id, seq, "PREPARE", node_id
    )
    await broadcast_to_peers(peers, "/prepare", payload)


async def _broadcast_commits(
    peers: list[str],
    private_key_hex: str,
    node_id: str,
    tx_id: str,
    seq: int,
):
    payload = build_signed_message(
        private_key_hex, tx_id, seq, "COMMIT", node_id
    )
    await broadcast_to_peers(peers, "/commit", payload)
