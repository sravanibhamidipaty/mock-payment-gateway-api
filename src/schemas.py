from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class ChargeRequest(BaseModel):
    amount: int = Field(gt=0, description="The price that is being charged in cents")
    currency: str = Field(
        min_length=3, max_length=3, description="3-letter currency code"
    )
    description: Optional[str] = None


class ChargeResponse(BaseModel):
    id: int
    user_id: int
    amount: int
    currency: str
    idempotency_key: str
    created_at: datetime
    model_config = ConfigDict(
        from_attributes=True
    )  # Allows Pydantic to read SQLAlchemy objects
