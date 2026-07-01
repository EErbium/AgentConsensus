from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel
from node_app.schemas.transaction import TransactionEnvelope


class LedgerEntry(BaseModel):
    ledger_index: int
    tx_id: str
    status: Literal["COMMITTED", "REJECTED"]
    validated_at: datetime
    envelope: TransactionEnvelope
    rejection_reason: Optional[str] = None
