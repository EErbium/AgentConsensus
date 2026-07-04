import time
from node_app.schemas.transaction import ExecutionPayload

MAX_ALLOCATION_LIMIT = 1000.00000000
MAX_CLOCK_DRIFT_SECONDS = 60


class InvariantViolation(Exception):
    def __init__(self, message: str, field_path: str, error_type: str):
        self.message = message
        self.field_path = field_path
        self.error_type = error_type
        super().__init__(message)


def check_fund_allocation_threshold(payload: ExecutionPayload) -> None:
    if payload.action != "ALLOCATE_FUNDS":
        return
        
    if payload.asset_amount <= MAX_ALLOCATION_LIMIT:
        return

    raise InvariantViolation(
        f"ALLOCATE_FUNDS amount {payload.asset_amount} exceeds maximum allowed limit of {MAX_ALLOCATION_LIMIT}",
        field_path="execution_payload.asset_amount",
        error_type="invariant.allocation_threshold",
    )


def check_timestamp_replay(packet_timestamp: int) -> None:
    clock_skew = abs(time.time() - packet_timestamp)
    if clock_skew <= MAX_CLOCK_DRIFT_SECONDS:
        return

    raise InvariantViolation(
        f"Timestamp {packet_timestamp} drifts {clock_skew:.0f}s from node system clock (max permitted {MAX_CLOCK_DRIFT_SECONDS}s)",
        field_path="timestamp",
        error_type="invariant.timestamp_drift",
    )


def validate_all(
    execution_payload: ExecutionPayload,
    packet_timestamp: int,
) -> list[InvariantViolation]:
    active_violations: list[InvariantViolation] = []
    
    try:
        check_fund_allocation_threshold(execution_payload)
    except InvariantViolation as threshold_error:
        active_violations.append(threshold_error)
        
    try:
        check_timestamp_replay(packet_timestamp)
    except InvariantViolation as replay_error:
        active_violations.append(replay_error)
        
    return active_violations