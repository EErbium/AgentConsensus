import asyncio
import json
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from node_app.schemas.transaction import TransactionEnvelope
from node_app.schemas.consensus import PrepareMessage, CommitMessage
from node_app.schemas.ledger_entry import LedgerEntry
from node_app.core.crypto import verify_signature, sign_payload
from node_app.core.consensus_tracker import ConsensusTracker
from node_app.core.mesh import build_signed_message, broadcast_to_peers, PHASE_COMMIT
from node_app.routers.chaos import FAULT_BYZANTINE

router = APIRouter()


async def consensus_watchdog(seq_num: int, tracker: ConsensusTracker, timeout_secs: float = 2.0):
    await asyncio.sleep(timeout_secs)
    stage = tracker.local_stage.get(seq_num)
    if stage not in ("COMMITTED", "EVICTED"):
        await tracker.force_garbage_collection(seq_num)


@router.post("/pre-prepare")
async def pre_prepare(
    envelope: TransactionEnvelope,
    background: BackgroundTasks,
    request: Request,
):
    state = request.app.state

    if getattr(state, "fault_mode", "") == FAULT_BYZANTINE:
        print("DEBUG: Sending corrupted hash now!")

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
    async with state.tracker.lock:
        if seq > state.tracker.local_head + 1:
            logging.warning(
                f"Node out of sync! Fast-forwarding local_head from "
                f"{state.tracker.local_head} to {seq - 1}"
            )
            for old_seq in list(state.tracker.local_stage.keys()):
                if old_seq < seq:
                    state.tracker.pre_prepares.pop(old_seq, None)
                    state.tracker.prepare_votes.pop(old_seq, None)
                    state.tracker.commit_votes.pop(old_seq, None)
            state.tracker.local_head = seq - 1
        state.tracker.pre_prepares[seq] = envelope
        state.tracker.local_stage[seq] = "PRE_PREPARED"

        # If prepare votes arrived before the pre-prepare, advance immediately
        if (
            seq in state.tracker.prepare_votes
            and len(state.tracker.prepare_votes[seq]) >= state.tracker.quorum
        ):
            state.tracker.local_stage[seq] = "PREPARED"
            background.add_task(
                _broadcast_commits,
                state.peers,
                state.private_key_hex,
                state.node_id,
                envelope.tx_id,
                seq,
            )

    background.add_task(consensus_watchdog, seq, state.tracker)

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

    # Isolate crypto check so a bad signature never crashes the node
    is_valid = False
    try:
        is_valid = _verify_phase_sig(pub_hex, msg.tx_id, msg.sequence_number, "PREPARE", msg.signature)
    except Exception as e:
        logging.error(f"Cryptographic decoding crashed for {msg.validator}: {e}")

    if not is_valid:
        logging.warning(f"Byzantine signature detected from malicious actor: {msg.validator}. Dropping vote.")
        return JSONResponse(
            status_code=202,
            content={"status": "vote_processed", "valid": False},
        )

    # Force strict serialization invariants
    incoming_tx_id = str(msg.tx_id).strip().lower()
    incoming_seq = int(msg.sequence_number)
    sender_identity = str(msg.validator).strip().lower()

    async with state.tracker.lock:
        # Atomic set copy-and-swap for thread safety
        existing = state.tracker.prepare_votes.get(incoming_seq)
        current_votes = set(existing) if existing is not None else set()

        local_record = state.tracker.pre_prepares.get(incoming_seq)
        if local_record:
            local_tx_id = str(local_record.tx_id).strip().lower()
            if incoming_tx_id == local_tx_id:
                current_votes.add(sender_identity)
            else:
                logging.error(
                    f"Hash Mismatch! Local: {local_tx_id} vs Incoming: {incoming_tx_id}"
                )
        else:
            # Race condition recovery: log the vote anyway
            current_votes.add(sender_identity)

        state.tracker.prepare_votes[incoming_seq] = current_votes
        total_valid_votes = len(current_votes)

        print(
            f"DEBUG Seq {incoming_seq}: Active PREPARE Votes -> "
            f"{current_votes}",
            flush=True,
        )

        if (
            total_valid_votes >= state.tracker.quorum
            and state.tracker.local_stage.get(incoming_seq) == "PRE_PREPARED"
        ):
            state.tracker.local_stage[incoming_seq] = "PREPARED"
            background.add_task(
                _broadcast_commits,
                state.peers,
                state.private_key_hex,
                state.node_id,
                msg.tx_id,
                incoming_seq,
            )

    return JSONResponse(
        status_code=202,
        content={"status": "vote_processed", "valid": True},
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

    # Isolate crypto check so a bad signature never crashes the node
    is_valid = False
    try:
        is_valid = _verify_phase_sig(pub_hex, msg.tx_id, msg.sequence_number, "COMMIT", msg.signature)
    except Exception as e:
        logging.error(f"Cryptographic decoding crashed for {msg.validator}: {e}")

    if not is_valid:
        logging.warning(f"Byzantine signature detected from malicious actor: {msg.validator}. Dropping commit.")
        return JSONResponse(
            status_code=202,
            content={"status": "vote_processed", "valid": False},
        )

    # Force strict serialization invariants
    incoming_tx_id = str(msg.tx_id).strip().lower()
    incoming_seq = int(msg.sequence_number)
    sender_identity = str(msg.validator).strip().lower()

    envelope_to_commit = None
    async with state.tracker.lock:
        # Atomic set copy-and-swap for thread safety
        existing = state.tracker.commit_votes.get(incoming_seq)
        current_votes = set(existing) if existing is not None else set()

        local_record = state.tracker.pre_prepares.get(incoming_seq)
        if local_record:
            local_tx_id = str(local_record.tx_id).strip().lower()
            if incoming_tx_id == local_tx_id:
                current_votes.add(sender_identity)
            else:
                logging.error(
                    f"Hash Mismatch! Local: {local_tx_id} vs Incoming: {incoming_tx_id}"
                )
        else:
            current_votes.add(sender_identity)

        state.tracker.commit_votes[incoming_seq] = current_votes
        total_valid_votes = len(current_votes)

        print(
            f"DEBUG Seq {incoming_seq}: Active COMMIT Votes -> "
            f"{current_votes}",
            flush=True,
        )

        if (
            total_valid_votes >= state.tracker.quorum
            and state.tracker.local_stage.get(incoming_seq) == "PREPARED"
        ):
            state.tracker.local_stage[incoming_seq] = "COMMITTED"
            state.tracker.local_head = incoming_seq
            envelope_to_commit = state.tracker.pre_prepares.get(incoming_seq)

    if envelope_to_commit is not None:
        async with state.ledger_lock:
            ledger: list[dict] = json.loads(
                state.ledger_path.read_text("utf-8")
            )
            entry = LedgerEntry(
                ledger_index=len(ledger) + 1,
                tx_id=envelope_to_commit.tx_id,
                status="COMMITTED",
                validated_at=datetime.now(timezone.utc),
                envelope=envelope_to_commit,
            )
            ledger.append(entry.model_dump(mode="json"))
            state.ledger_path.write_text(
                json.dumps(ledger, indent=2, default=str, ensure_ascii=False),
                encoding="utf-8",
            )

    return JSONResponse(
        status_code=202,
        content={"status": "vote_processed", "valid": True},
    )


@router.get("/verify-tx/{seq_num}")
async def verify_transaction_in_memory(seq_num: int, request: Request):
    """Fast-path verification — reads from in-memory tracker, no disk I/O."""
    state = request.app.state
    async with state.tracker.lock:
        current_stage = state.tracker.local_stage.get(seq_num)
        committed = current_stage == "COMMITTED"
        return {"committed": committed, "node": state.node_id}


@router.post("/reset-head")
async def reset_head(request: Request, body: dict):
    target_sequence = body.get("target_sequence")
    if target_sequence is None:
        return JSONResponse(status_code=422, content={"detail": "target_sequence required"})
    state = request.app.state
    async with state.tracker.lock:
        state.tracker.local_head = target_sequence
    return {"status": "synchronized", "local_head": target_sequence}


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
