import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

load_dotenv()

# 1. Grab the URL from the environment (Docker injects DATABASE_URL).
# 2. When running on the host (no DATABASE_URL), fall back to DATABASE_URL_LOCAL.
# Both come from .env so no credentials are ever hardcoded here.
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_LOCAL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL (or DATABASE_URL_LOCAL) must be set. See .env.example."
    )

# The engine is the core connection to the database
# echo=True prints the generated SQL to your terminal (great for debugging!)
engine = create_async_engine(DATABASE_URL, echo=True)

# The sessionmaker creates individual "conversations" with the database
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# This is the Base class that our models.py inherited from!
Base = declarative_base()


# A helpful dependency function we will use in main.py to get a database session
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
