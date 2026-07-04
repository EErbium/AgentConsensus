from pydantic import BaseModel, Field, field_validator


class PrepareMessage(BaseModel):
    tx_id: str = Field(..., min_length=64, max_length=64)
    sequence_number: int = Field(..., ge=0)
    validator: str = Field(..., min_length=1)
    signature: str = Field(..., min_length=1)

    @field_validator("tx_id", mode="before")
    @classmethod
    def clean_transaction_hash(cls, raw_value: str) -> str:
        if isinstance(raw_value, str):
            return raw_value.strip().lower()
        return raw_value


class CommitMessage(BaseModel):
    tx_id: str = Field(..., min_length=64, max_length=64)
    sequence_number: int = Field(..., ge=0)
    validator: str = Field(..., min_length=1)
    signature: str = Field(..., min_length=1)

    @field_validator("tx_id", mode="before")
    @classmethod
    def clean_transaction_hash(cls, raw_value: str) -> str:
        if isinstance(raw_value, str):
            return raw_value.strip().lower()
        return raw_value