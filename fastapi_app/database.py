from clickhouse_sqlalchemy import get_declarative_base
from contextlib import asynccontextmanager, contextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from collections.abc import AsyncGenerator
from sqlalchemy.orm import sessionmaker
from clickhouse_sqlalchemy import get_declarative_base
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker


asynch_uri = "clickhouse+asynch://galileo:observe@127.0.0.1:19000/galileo_chdb"
asynch_engine = create_async_engine(asynch_uri)
AsyncSessionLocal = sessionmaker(bind=asynch_engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore[call-overload]
db_write_engine = create_engine( "clickhouse+native://galileo:observe@127.0.0.1:19000/galileo_chdb")
SessionLocalWriter = scoped_session(sessionmaker(bind=db_write_engine, class_=Session))

async def clickhouse_client_async_session() -> AsyncGenerator:
    try:
        async with AsyncSessionLocal() as session:
            yield session
    except Exception as e:
        raise e
    finally:
        session.close()


async_clickhouse_client = asynccontextmanager(clickhouse_client_async_session())


### Dependencies
async def get_db_async():
    try:
        async with AsyncSessionLocal() as session:
            yield session
    except Exception as e:
        raise e
    finally:
        await session.close()

def get_db_sync():
    db = SessionLocalWriter()
    try:
        yield db
    except Exception as e:
        raise e
    finally:
        db.close()