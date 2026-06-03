import asyncio
import json
import os
import httpx
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession
from database import engine
import models

load_dotenv()

KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")
NOTIFIER_URL = os.getenv("NOTIFIER_URL", "http://notifier:8001/webhook")


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

    for attempt in range(10):
        try:
            await consumer.start()
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
                new_charge = models.Charge(
                    user_id=payment_box["user_id"],
                    amount=payment_box["amount"],
                    currency=payment_box["currency"],
                    idempotency_key=payment_box["idempotency_key"],
                )
                db.add(new_charge)

                try:
                    await db.commit()
                    print(
                        f"✅ Worker: Successfully saved charge '{payment_box['idempotency_key']}'!"
                    )

                    # ---> NEW: DIRECT SERVICE-TO-SERVICE WEBHOOK! <---
                    async with httpx.AsyncClient() as client:
                        await client.post(
                            NOTIFIER_URL,
                            json={
                                "amount": payment_box["amount"],
                                "currency": payment_box["currency"],
                                "user_id": payment_box["user_id"],
                            },
                        )
                    print("📬 Worker: Fired Direct Webhook to Notifier!\n")

                except Exception as e:
                    await db.rollback()
                    print(f"❌ Worker: Failed to save to database. Error: {e}\n")

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
