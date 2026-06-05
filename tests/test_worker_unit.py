import pytest
from unittest.mock import AsyncMock, patch

import worker

BOX = {"user_id": 1, "amount": 100, "currency": "USD", "idempotency_key": "k"}


@pytest.mark.asyncio
async def test_send_notification_retries_then_succeeds():
    """Webhook fails twice, succeeds on the third attempt — no exception raised."""
    with (
        patch("worker.httpx.AsyncClient") as mock_client,
        patch("worker.asyncio.sleep", new_callable=AsyncMock),
    ):
        client = mock_client.return_value.__aenter__.return_value = AsyncMock()
        client.post.side_effect = [Exception("boom"), Exception("boom"), None]

        await worker.send_notification(BOX)

        assert client.post.await_count == 3


@pytest.mark.asyncio
async def test_send_notification_raises_after_exhausting_retries():
    """Webhook always fails — raises after MAX_WEBHOOK_RETRIES attempts."""
    with (
        patch("worker.httpx.AsyncClient") as mock_client,
        patch("worker.asyncio.sleep", new_callable=AsyncMock),
    ):
        client = mock_client.return_value.__aenter__.return_value = AsyncMock()
        client.post.side_effect = Exception("always fails")

        with pytest.raises(Exception):
            await worker.send_notification(BOX)

        assert client.post.await_count == worker.MAX_WEBHOOK_RETRIES


@pytest.mark.asyncio
async def test_send_to_dlq_publishes_original_and_error():
    """Dead-lettering wraps the original message plus the error and publishes it."""
    producer = AsyncMock()

    await worker.send_to_dlq(producer, BOX, ValueError("bad data"))

    producer.send_and_wait.assert_awaited_once()
    topic, raw = producer.send_and_wait.await_args.args
    assert topic == worker.DLQ_TOPIC

    import json

    payload = json.loads(raw.decode("utf-8"))
    assert payload["original"] == BOX
    assert "bad data" in payload["error"]
