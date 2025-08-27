import logging

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker

from app.core.settings import settings

logger = logging.getLogger(__name__)

try:
    logger.info(f"Initializing database connection to {settings.PG_HOST}:{settings.PG_PORT}")

    engine: AsyncEngine = create_async_engine(
        settings.get_postgres_dsn,
        echo=settings.PG_ECHO,
        pool_pre_ping=settings.PG_POOL_PRE_PING,
        pool_recycle=settings.PG_POOL_RECYCLE,
        pool_size=settings.PG_POOL_SIZE,
        max_overflow=settings.PG_MAX_OVERFLOW,
    )

    async_session = async_sessionmaker(
        bind=engine,
        expire_on_commit=settings.DB_EXPIRE_ON_COMMIT,
        autoflush=settings.DB_AUTOFLUSH,
        autocommit=settings.DB_AUTOCOMMIT,
    )

    logger.info("Database engine and session maker created successfully")

except SQLAlchemyError as e:
    logger.error(f"Failed to create database engine: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error during database initialization: {e}")
    raise