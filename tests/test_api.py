import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from src.main import app  # Import your actual FastAPI app


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
            "/users/999/charges", headers={"x-api-key": "sk_test_12345"}
        )

    assert response.status_code == 200


# ==========================================
# TEST 4: Create Charge (Idempotency 200)
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
            headers={"x-api-key": "sk_test_12345", "idempotency-key": random_key},
            json=payload,
        )

    assert response.status_code == 200
