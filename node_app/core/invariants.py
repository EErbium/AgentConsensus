import time
from node_app.schemas.transaction import ExecutionPayload

MAX_ALLOCATE_AMOUNT = 1000.00000000
MAX_TIMESTAMP_DRIFT_SECONDS = 60


class InvariantViolation(Exception):
    def __init__(self, message: str, field_path: str, error_type: str):
        self.message = message
        self.field_path = field_path
        self.error_type = error_type
        super().__init__(message)


def check_fund_allocation_threshold(payload: ExecutionPayload) -> None:
    if payload.action == "ALLOCATE_FUNDS" and payload.asset_amount > MAX_ALLOCATE_AMOUNT:
        raise InvariantViolation(
            f"ALLOCATE_FUNDS amount {payload.asset_amount} exceeds "
            f"maximum {MAX_ALLOCATE_AMOUNT}",
            field_path="execution_payload.asset_amount",
            error_type="invariant.allocation_threshold",
        )


def check_timestamp_replay(timestamp: int) -> None:
    drift = abs(time.time() - timestamp)
    if drift > MAX_TIMESTAMP_DRIFT_SECONDS:
        raise InvariantViolation(
            f"Timestamp {timestamp} drifts {drift:.0f}s from system clock "
            f"(max {MAX_TIMESTAMP_DRIFT_SECONDS}s)",
            field_path="timestamp",
            error_type="invariant.timestamp_drift",
        )


def validate_all(
    execution_payload: ExecutionPayload,
    timestamp: int,
) -> list[InvariantViolation]:
    violations: list[InvariantViolation] = []
    try:
        check_fund_allocation_threshold(execution_payload)
    except InvariantViolation as e:
        violations.append(e)
    try:
        check_timestamp_replay(timestamp)
    except InvariantViolation as e:
        violations.append(e)
    return violations
