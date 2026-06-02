from fastapi import FastAPI, Header, HTTPException, status, Depends
from schemas import ChargeRequest, ChargeResponse
from sqlalchemy.exc import IntegrityError
import models
from database import get_db, engine, Base
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from sqlalchemy import select
import os
from dotenv import load_dotenv
import redis.asyncio as redis

load_dotenv()

app = FastAPI(description="Mock Payment Gateway API")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
# decode_responses=True ensures we get clean strings back, not raw bytes
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

def verify_api_key(x_api_key: str = Header(..., description="API Key for Authorization")):
    # Step 2: Check if it matches our secret
    real_secret = os.getenv("API_KEY_SECRET")
    if x_api_key != real_secret:
        # Step 3 & 4 The Rejection
        # We don't return a JSON, we RAISE an exception.
        # FastAPI catches this and automatically builds the 401 JSON for the user!
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key. You aren't allowed here."
        )

    # If it matches, Python just skips the 'if' block and finishes the function.
    # That means "Access Granted!"

@app.on_event("startup")
async def startup_event():
    # This reaches out to the DB and creates any tables defined in models.py
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("--- Database initialized and tables created!")

@app.post("/users/{user_id}/charges", dependencies=[Depends(verify_api_key)])
async def create_charge(
        user_id: int,
        charge_request: ChargeRequest,
        # We use Header(...) to make it required.
        # FastAPI automatically converts 'idempotency_key' to the HTTP standard 'Idempotency-Key'
        idempotency_key: str = Header(..., description="Unique hash to prevent double charges"),
        db: AsyncSession = Depends(get_db),
):

    # We check memory before we ever touch the database.
    is_duplicate = await redis_client.get(idempotency_key)
    if is_duplicate:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Idempotency Key '{idempotency_key}' has already been used. (Caught by Redis!)",
        )

    try:
        # 1. Translate the Pydantic Request into a SQLALchemy Database Model
        new_charge = models.Charge(
            user_id=user_id,
            amount=charge_request.amount,
            currency=charge_request.currency,
            idempotency_key=idempotency_key
        )

        # 2. Stage it and save it to PostgreSQL
        db.add(new_charge)
        await db.commit()
        await db.refresh(new_charge) # Refreshes to get the auto-generated ID and created_at

        # The database finished! Tell Redis to remember this key for 24 hours (86400 seconds)
        await redis_client.set(idempotency_key, "processed", ex=86400)

        # 3. Return the newly saved row!
        return new_charge

    except IntegrityError:
        # The database blocked the duplicate! Rollback the failed transaction.
        await db.rollback()

        # Now, raise the FastAPI HTTP Exception to tell the user!
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Idempotency Key '{idempotency_key}' has already been used. (Caught by DB)",
        )

@app.get("/users/{user_id}/charges", response_model=List[ChargeResponse], dependencies=[Depends(verify_api_key)])
async def get_user_charges(user_id: int, db: AsyncSession = Depends(get_db)):
    query = select(models.Charge).where(models.Charge.user_id == user_id)
    result = await db.execute(query)
    return result.scalars().all()