import hashlib
import json
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


class ExecutionPayload(BaseModel):
    action: Literal["ALLOCATE_FUNDS", "MUTATE_MODEL_WEIGHTS", "EXECUTE_COMPUTE"]
    target_recipient: str = Field(..., min_length=1)
    asset_amount: float = Field(..., ge=0)
    denomination: str = Field(..., min_length=1)

    @field_validator("asset_amount")
    @classmethod
    def eight_decimal_precision(cls, v: float) -> float:
        d = Decimal(str(v))
        if d.as_tuple().exponent < -8:
            raise ValueError(
                f"asset_amount {v} exceeds 8-decimal precision limit"
            )
        return float(d.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN))


class TransactionEnvelope(BaseModel):
    tx_id: str
    proposer_node: str
    sequence_number: int = Field(..., ge=0)
    timestamp: int
    execution_payload: ExecutionPayload
    llm_reasoning_hash: str
    signatures: Dict[str, str]

    @field_validator("tx_id", "llm_reasoning_hash")
    @classmethod
    def validate_sha256_hex(cls, v: str) -> str:
        if len(v) != 64:
            raise ValueError("must be a 64-character hex SHA-256 string")
        try:
            int(v, 16)
        except ValueError:
            raise ValueError("must be a valid hex string")
        return v

    @model_validator(mode="after")
    def verify_tx_id(self):
        canonical = json.dumps(
            self.execution_payload.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        raw = (
            self.proposer_node
            + str(self.sequence_number)
            + str(self.timestamp)
            + canonical
        )
        expected = hashlib.sha256(raw.encode()).hexdigest()
        if self.tx_id != expected:
            raise ValueError(
                f"tx_id {self.tx_id} does not match computed hash {expected}"
            )
        return self
