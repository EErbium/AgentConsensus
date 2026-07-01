from pydantic import BaseModel, Field


class PrepareMessage(BaseModel):
    tx_id: str = Field(..., min_length=64, max_length=64)
    sequence_number: int = Field(..., ge=0)
    validator: str = Field(..., min_length=1)
    signature: str = Field(..., min_length=1)


class CommitMessage(BaseModel):
    tx_id: str = Field(..., min_length=64, max_length=64)
    sequence_number: int = Field(..., ge=0)
    validator: str = Field(..., min_length=1)
    signature: str = Field(..., min_length=1)
