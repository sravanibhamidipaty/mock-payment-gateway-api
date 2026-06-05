from fastapi import FastAPI, Header, HTTPException, status, Depends
from schemas import ChargeRequest, ChargeResponse
import models
from database import get_db, engine, Base
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from sqlalchemy import select
import os
from dotenv import load_dotenv
import redis.asyncio as redis
import json
from aiokafka import AIOKafkaProducer
import asyncio

load_dotenv()

app = FastAPI(description="Mock Payment Gateway API")

KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")
kafka_producer = None  # We will turn this on when the server starts

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
# decode_responses=True ensures we get clean strings back, not raw bytes
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


def verify_api_key(
    x_api_key: str = Header(..., description="API Key for Authorization"),
):
    # Step 2: Check if it matches our secret
    real_secret = os.getenv("API_KEY_SECRET")
    if x_api_key != real_secret:
        # Step 3 & 4 The Rejection
        # We don't return a JSON, we RAISE an exception.
        # FastAPI catches this and automatically builds the 401 JSON for the user!
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key. You aren't allowed here.",
        )

    # If it matches, Python just skips the 'if' block and finishes the function.
    # That means "Access Granted!"


@app.on_event("startup")
async def startup_event():
    global kafka_producer

    # This reaches out to the DB and creates any tables defined in models.py
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("--- Database initialized and tables created!")

    # THE FIX: Give Kafka time to boot up! Try to connect 10 times, waiting 3 seconds between each try.
    for attempt in range(10):
        try:
            # We put the connection INSIDE the try block to get a fresh start every time
            kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
            await kafka_producer.start()
            print("--- Successfully connected to the Kafka Conveyor Belt!")
            break  # If successful, break out of the loop!
        except Exception:
            print(
                f"--- Kafka not ready yet (Attempt {attempt + 1}/10). Waiting 3 seconds..."
            )
            await asyncio.sleep(3)
    else:
        # If it fails all 10 times, we purposefully crash the server so it doesn't fail silently
        raise Exception("Fatal Error: Could not connect to Kafka after 30 seconds.")


@app.on_event("shutdown")
async def shutdown_event():
    # Safely turn off the motor when the server stops
    if kafka_producer:
        await kafka_producer.stop()


# We change the status code to 202 Accepted (Meaning: "We got it, we are working on it in the background")
@app.post(
    "/users/{user_id}/charges",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_api_key)],
)
async def create_charge(
    user_id: int,
    charge_request: ChargeRequest,
    # We use Header(...) to make it required.
    # FastAPI automatically converts 'idempotency_key' to the HTTP standard 'Idempotency-Key'
    idempotency_key: str = Header(
        ..., description="Unique hash to prevent double charges"
    ),
    # Notice we completely removed the 'db' dependency here. The Waiter no longer talks to the Chef!
):
    # We check memory before we ever touch the database.
    is_duplicate = await redis_client.get(idempotency_key)
    if is_duplicate:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Idempotency Key '{idempotency_key}' has already been used. (Caught by Redis!)",
        )

    # 1. Package the order into a JSON box
    payment_box = {
        "user_id": user_id,
        "amount": charge_request.amount,
        "currency": charge_request.currency,
        "idempotency_key": idempotency_key,
    }

    # 2. Drop the box onto the Conveyor Belt! (Topic name: 'payments')
    # We have to encode the string into bytes before putting it on the belt
    if kafka_producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer is not ready yet.",
        )
    await kafka_producer.send_and_wait(
        "payments", json.dumps(payment_box).encode("utf-8")
    )

    # 3. Tell the Bouncer to remember this key for 24 hours (86400 seconds)
    await redis_client.set(idempotency_key, "processed", ex=86400)

    # 4. Instantly return to the user! (No database waiting)
    return {
        "status": "processing",
        "message": "Payment dropped on the conveyor belt!",
        "idempotency_key": idempotency_key,
    }


@app.get(
    "/users/{user_id}/charges",
    response_model=List[ChargeResponse],
    dependencies=[Depends(verify_api_key)],
)
async def get_user_charges(user_id: int, db: AsyncSession = Depends(get_db)):
    query = select(models.Charge).where(models.Charge.user_id == user_id)
    result = await db.execute(query)
    return result.scalars().all()
