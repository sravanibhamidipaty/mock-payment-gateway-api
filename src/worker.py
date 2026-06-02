import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)  # <-- NEW: Import this from SQLAlchemy directly
from database import (
    engine,
)  # <-- CHANGED: We import the engine instead of guessing the session maker name
import models

KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")


async def consume():
    # 1. The Worker puts on his uniform and prepares to watch the 'payments' belt
    consumer = AIOKafkaConsumer(
        "payments",
        bootstrap_servers=KAFKA_URL,
        group_id="payment-processors",
        # This tells the worker how to open the JSON boxes
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # 2. Give the Worker patience (Retry loop just like the Waiter)
    for attempt in range(10):
        try:
            await consumer.start()
            print("👷 Worker: Successfully connected to the Conveyor Belt!")
            break
        except Exception:
            print(
                f"👷 Worker: Belt not ready yet (Attempt {attempt + 1}/10). Waiting 3 seconds..."
            )
            await asyncio.sleep(3)
    else:
        raise Exception("Fatal Error: Worker could not connect to Kafka.")

    try:
        print("👷 Worker: Standing by, waiting for payment boxes...")

        # 3. The Infinite Loop! The worker stands here forever grabbing boxes.
        async for msg in consumer:
            payment_box = msg.value
            print(
                f"👷 Worker: Picked up a box! Processing payment for User {payment_box['user_id']}..."
            )

            # 4. Hand the data to the Chef (PostgreSQL) using the Engine directly!
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
                        f"✅ Worker: Successfully saved charge '{payment_box['idempotency_key']}' to the database!\n"
                    )
                except Exception as e:
                    await db.rollback()
                    print(f"❌ Worker: Failed to save to database. Error: {e}\n")

    finally:
        await consumer.stop()


if __name__ == "__main__":
    # Wake up the worker!
    asyncio.run(consume())
