import asyncio
import json
import os
import aioboto3
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession
from database import engine
import models

KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://floci:4566")


async def consume():
    # 1. Wake up the AWS Megaphone (SNS)
    aws_session = aioboto3.Session()
    async with aws_session.client(
        "sns",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as sns_client:
        # We tell Floci to create our specific Topic
        topic = await sns_client.create_topic(Name="payment-notifications")
        sns_topic_arn = topic["TopicArn"]
        print(f"📡 Worker: AWS SNS Megaphone Ready! ({sns_topic_arn})")

        # 2. Start the Kafka Consumer
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

                # 3. Hand the data to the Chef (PostgreSQL)
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

                        # 4. NEW: Broadcast the success to AWS SNS!
                        await sns_client.publish(
                            TopicArn=sns_topic_arn,
                            Message=json.dumps(
                                {
                                    "event": "payment_succeeded",
                                    "user_id": payment_box["user_id"],
                                    "amount": payment_box["amount"],
                                    "currency": payment_box["currency"],
                                }
                            ),
                        )
                        print("📬 Worker: Fired SNS Webhook Notification!\n")

                    except Exception as e:
                        await db.rollback()
                        print(f"❌ Worker: Failed to save to database. Error: {e}\n")

        finally:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
