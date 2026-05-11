from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

# Format: postgresql+asyncpg://user:password@host:port/database_name
DATABASE_URL = "postgresql+asyncpg://my_db_user:supersecret@localhost:5432/payment_gateway"

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