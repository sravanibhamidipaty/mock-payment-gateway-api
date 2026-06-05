import asyncio
import json
import os
import httpx
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession
from database import engine
import models

load_dotenv()

KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")
NOTIFIER_URL = os.getenv("NOTIFIER_URL", "http://notifier:8001/webhook")

# Resilience knobs
MAX_WEBHOOK_RETRIES = int(os.getenv("MAX_WEBHOOK_RETRIES", "3"))
WEBHOOK_BACKOFF_BASE = float(os.getenv("WEBHOOK_BACKOFF_BASE", "1.0"))
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "payments.DLQ")


async def send_notification(payment_box):
    """POST to the notifier with exponential backoff. Raises if all attempts fail."""
    payload = {
        "amount": payment_box["amount"],
        "currency": payment_box["currency"],
        "user_id": payment_box["user_id"],
    }
    delay = WEBHOOK_BACKOFF_BASE
    async with httpx.AsyncClient(timeout=5.0) as client:
        for attempt in range(1, MAX_WEBHOOK_RETRIES + 1):
            try:
                await client.post(NOTIFIER_URL, json=payload)
                return
            except Exception:
                if attempt == MAX_WEBHOOK_RETRIES:
                    raise  # exhausted retries — let the caller decide
                await asyncio.sleep(delay)
                delay *= 2  # exponential backoff: 1s, 2s, 4s, ...


async def persist_charge(db, payment_box):
    """Write the charge to PostgreSQL in a transaction."""
    new_charge = models.Charge(
        user_id=payment_box["user_id"],
        amount=payment_box["amount"],
        currency=payment_box["currency"],
        idempotency_key=payment_box["idempotency_key"],
    )
    db.add(new_charge)
    await db.commit()
    return new_charge


async def process_payment(db, payment_box):
    """Persist the charge, then best-effort notify.

    Persistence is the source of truth: if it fails, the exception propagates so
    the message can be dead-lettered. The notification is best-effort — once the
    charge is durably saved, a failed receipt is logged but does not fail the
    message (it would otherwise be replayed and violate the unique idempotency key).
    """
    charge = await persist_charge(db, payment_box)
    try:
        await send_notification(payment_box)
    except Exception as exc:
        print(
            f"⚠️ Worker: charge '{payment_box['idempotency_key']}' saved, "
            f"but notification failed after {MAX_WEBHOOK_RETRIES} retries: {exc}"
        )
    return charge


async def send_to_dlq(producer, payment_box, error):
    """Route an unprocessable ('poison') message to the dead-letter topic."""
    dead_letter = {"original": payment_box, "error": str(error)}
    await producer.send_and_wait(DLQ_TOPIC, json.dumps(dead_letter).encode("utf-8"))


async def consume():
    print("⏳ Worker: Waiting 5 seconds for Notifier to boot up...")
    await asyncio.sleep(5)

    # 1. Start the Kafka Consumer
    consumer = AIOKafkaConsumer(
        "payments",
        bootstrap_servers=KAFKA_URL,
        group_id="payment-processors",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    # 2. A producer so we can dead-letter messages we can't process
    dlq_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)

    for attempt in range(10):
        try:
            await consumer.start()
            await dlq_producer.start()
            break
        except Exception:
            await asyncio.sleep(3)
    else:
        raise Exception("Fatal Error: Worker could not connect to Kafka.")

    try:
        print("👷 Worker: Standing by, waiting for payment boxes...")

        async for msg in consumer:
            payment_box = msg.value

            async with AsyncSession(engine) as db:
                try:
                    await process_payment(db, payment_box)
                    print(
                        f"✅ Worker: Saved charge '{payment_box['idempotency_key']}' and fired webhook!\n"
                    )
                except Exception as exc:
                    # Persistence/processing failed — don't drop the message, dead-letter it.
                    await db.rollback()
                    await send_to_dlq(dlq_producer, payment_box, exc)
                    print(
                        f"☠️ Worker: routed message to DLQ '{DLQ_TOPIC}'. Error: {exc}\n"
                    )

    finally:
        await consumer.stop()
        await dlq_producer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
