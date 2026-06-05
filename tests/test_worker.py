import asyncio

import pytest
from unittest.mock import AsyncMock, patch

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import models
import worker

# Skip cleanly if testcontainers isn't installed or the Docker daemon is down.
pytest.importorskip("testcontainers.postgres")
try:
    import docker

    docker.from_env().ping()
except Exception:
    pytest.skip("Docker daemon not available", allow_module_level=True)

from testcontainers.postgres import PostgresContainer  # noqa: E402


@pytest.mark.asyncio
async def test_worker_persists_charge_and_fires_webhook():
    with PostgresContainer("postgres:15") as pg:
        # testcontainers hands back a sync (psycopg2) URL; convert to async.
        url = pg.get_connection_url().replace("psycopg2", "asyncpg")
        engine = create_async_engine(url)

        # The container's port is open before Postgres fully accepts connections,
        # so retry briefly to keep this test from flaking on cold starts.
        for attempt in range(10):
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                break
            except Exception:
                if attempt == 9:
                    raise
                await asyncio.sleep(1)

        # Create the schema
        async with engine.begin() as conn:
            await conn.run_sync(models.Base.metadata.create_all)

        box = {
            "user_id": 1,
            "amount": 100,
            "currency": "USD",
            "idempotency_key": "test-key-1",
        }

        # Don't hit a real notifier — assert the webhook is fired.
        with patch("worker.httpx.AsyncClient") as mock_client:
            posted = mock_client.return_value.__aenter__.return_value = AsyncMock()
            async with AsyncSession(engine) as db:
                await worker.process_payment(db, box)
            posted.post.assert_awaited_once()

        # Verify the row really landed in Postgres.
        async with AsyncSession(engine) as db:
            result = await db.execute(
                select(models.Charge).where(
                    models.Charge.idempotency_key == "test-key-1"
                )
            )
            charge = result.scalar_one()
            assert charge.amount == 100
            assert charge.currency == "USD"

        await engine.dispose()
