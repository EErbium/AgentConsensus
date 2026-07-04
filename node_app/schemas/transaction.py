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

    @field_validator("asset_amount", mode="before")
    @classmethod
    def enforce_eight_decimal_precision(cls, raw_amount: float) -> float:
        # Float precision drifts during transport, so clamp to 8 decimals immediately via Decimal
        quantized = Decimal(str(raw_amount)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if quantized < 0:
            raise ValueError("Asset allocation amount cannot evaluate to a negative value.")
        return float(quantized)


class TransactionEnvelope(BaseModel):
    tx_id: str
    proposer_node: str
    sequence_number: int = Field(..., ge=0)
    timestamp: int
    execution_payload: ExecutionPayload
    llm_reasoning_hash: str
    signatures: Dict[str, str]

    @field_validator("tx_id", "llm_reasoning_hash", mode="before")
    @classmethod
    def clean_and_verify_sha256(cls, hex_string: str) -> str:
        if not isinstance(hex_string, str):
            raise ValueError("Target cryptographic digest must be presented as a raw string.")
        
        cleaned_hash = hex_string.strip().lower()
        if len(cleaned_hash) != 64:
            raise ValueError("Digest length mismatch: SHA-256 strings must be exactly 64 characters.")
        
        try:
            int(cleaned_hash, 16)
        except ValueError:
            raise ValueError("Malformed encoding: Digest contains non-hexadecimal symbols.")
            
        return cleaned_hash

    @model_validator(mode="after")
    def verify_tx_id(self):
        # Deterministic serialization format for structural identity confirmation across consensus nodes
        canonical_payload = json.dumps(
            self.execution_payload.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        
        preimage_string = (
            str(self.proposer_node).strip().lower()
            + str(self.sequence_number)
            + str(self.timestamp)
            + canonical_payload
        )
        
        computed_hash = hashlib.sha256(preimage_string.encode()).hexdigest()
        if self.tx_id != computed_hash:
            raise ValueError(
                f"Transaction identification discrepancy: Received '{self.tx_id}', but "
                f"cryptographic confirmation yielded '{computed_hash}'."
            )
            
        return self