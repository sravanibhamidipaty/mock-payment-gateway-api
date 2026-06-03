import os
import pytest
import uuid
from dotenv import load_dotenv
from httpx import AsyncClient, ASGITransport
from src.main import app  # Import your actual FastAPI app
from unittest.mock import AsyncMock, patch

load_dotenv()

# Pull the valid key from the environment (.env locally, GitHub Secrets in CI)
# so no real credentials are hardcoded in the test suite.
API_KEY = os.environ["API_KEY_SECRET"]

@pytest.fixture(autouse=True)
def mock_external_services():
    """
    This intercepts any calls to Redis and Kafka during tests and replaces them
    with a fake "AsyncMock" that always succeeds instantly!
    """
    with (
        patch("src.main.redis_client.get", new_callable=AsyncMock) as mock_get,
        patch("src.main.redis_client.set", new_callable=AsyncMock),
        # THE FIX: We patch the ENTIRE kafka_producer object, not just the method!
        patch("src.main.kafka_producer", new_callable=AsyncMock),
    ):
        # Tell the fake Redis that the key is NEVER a duplicate
        mock_get.return_value = None
        yield

# ==========================================
# TEST 1: The Missing Key (Front Gate 422)
# ==========================================
@pytest.mark.asyncio
async def test_bouncer_rejects_missing_key():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/users/999/charges")

    assert response.status_code == 422


# ==========================================
# TEST 2: The Fake Key (Bouncer 401)
# ==========================================
@pytest.mark.asyncio
async def test_bouncer_rejects_fake_key():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get(
            "/users/999/charges", headers={"x-api-key": "some-fake-password"}
        )

    assert response.status_code == 401


# ==========================================
# TEST 3: The VIP Pass (Success 200)
# ==========================================
@pytest.mark.asyncio
async def test_vip_pass_succeeds():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get(
            "/users/999/charges", headers={"x-api-key": API_KEY}
        )

    assert response.status_code == 200


# ==========================================
# TEST 4: Create Charge (Idempotency 202)
# ==========================================
@pytest.mark.asyncio
async def test_create_charge_success():
    # Generate a brand new, random string so the DB never rejects it
    random_key = str(uuid.uuid4())

    payload = {"amount": 15, "currency": "USD", "description": "Test Robot Charge"}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post(
            "/users/999/charges",
            headers={"x-api-key": API_KEY, "idempotency-key": random_key},
            json=payload,
        )

    # We expect a 202 Accepted now that we are using the Conveyor Belt!
    assert response.status_code == 202
    # Let's also verify it sends the correct JSON message
    assert response.json()["status"] == "processing"