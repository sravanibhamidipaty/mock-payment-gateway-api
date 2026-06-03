from fastapi import FastAPI, Request
import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="Notification Service")

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")


@app.post("/webhook")
async def receive_webhook(request: Request):
    # 1. Safely open the direct webhook from the Worker
    data = await request.json()
    amount = data.get("amount")
    currency = data.get("currency")

    print(f"📬 Notifier: Caught Direct Webhook for {amount} {currency}!")

    # 2. Draft the Real Email
    msg = EmailMessage()
    msg.set_content(
        f"Success! Your payment of ${amount} {currency} has been securely processed by the Kafka Factory."
    )
    msg["Subject"] = "Payment Receipt 🧾"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    try:
        print("📨 Notifier: Connecting to Google Servers...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)

        print("✅ Notifier: REAL EMAIL SUCCESSFULLY SENT!")

    except Exception as e:
        print(f"❌ Notifier: Failed to send email. Error: {e}")

    return {"status": "Notification Processed!"}
