from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from database import Base


class Charge(Base):
    __tablename__ = "charges"

    # The database generates this safe internal ID
    id = Column(Integer, primary_key=True, index=True)

    # The Idempotency Key with the UNIQUE constraint!
    idempotency_key = Column(String, unique=True, index=True, nullable=False)

    # The business data
    amount = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False)
    user_id = Column(Integer, index=True, nullable=False)

    # The auto-generating timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now())
