from fastapi import FastAPI, Header, HTTPException, status, Depends
from schemas import ChargeRequest, ChargeResponse
from sqlalchemy.exc import IntegrityError
import models
from database import get_db, engine, Base
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from sqlalchemy import select

app = FastAPI(description="Mock Payment Gateway API")

@app.on_event("startup")
async def startup_event():
    # This reaches out to the DB and creates any tables defined in models.py
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("--- Database initialized and tables created!")

@app.post("/users/{user_id}/charges")
async def create_charge(
        user_id: int,
        charge_request: ChargeRequest,
        # We use Header(...) to make it required.
        # FastAPI automatically converts 'idempotency_key' to the HTTP standard 'Idempotency-Key'
        idempotency_key: str = Header(..., description="Unique hash to prevent double charges"),
        db: AsyncSession = Depends(get_db)
):
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

        # 3. Return the newly saved row!
        return new_charge

    except IntegrityError:
        # The database blocked the duplicate! Rollback the failed transaction.
        await db.rollback()

        # Now, raise the FastAPI HTTP Exception to tell the user!
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Idempotency Key '{idempotency_key}' has already been used.",
        )

@app.get("/users/{user_id}/charges", response_model=List[ChargeResponse])
async def get_user_charges(user_id: int, db: AsyncSession = Depends(get_db)):
    query = select(models.Charge).where(models.Charge.user_id == user_id)
    result = await db.execute(query)
    return result.scalars().all()