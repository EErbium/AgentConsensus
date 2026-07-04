from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator
from node_app.schemas.transaction import TransactionEnvelope


class LedgerEntry(BaseModel):
    ledger_index: int = Field(..., gt=0)
    tx_id: str = Field(..., min_length=64, max_length=64)
    status: Literal["COMMITTED", "REJECTED"]
    validated_at: datetime
    envelope: TransactionEnvelope
    rejection_reason: Optional[str] = None

    @field_validator("tx_id", mode="before")
    @classmethod
    def normalize_tx_hash(cls, raw_hash: str) -> str:
        if isinstance(raw_hash, str):
            return raw_hash.strip().lower()
        return raw_hash

    @field_validator("validated_at", mode="before")
    @classmethod
    def enforce_utc_timezone(cls, raw_dt) -> datetime:
        if isinstance(raw_dt, str):
            # Parse ISO string and handle potential zulu offset quirks safely
            raw_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
        if raw_dt.tzinfo is None:
            from datetime import timezone
            return raw_dt.replace(tzinfo=timezone.utc)
        return raw_dt