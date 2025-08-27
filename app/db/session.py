import logging
from typing import AsyncGenerator

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import async_session

logger = logging.getLogger(__name__)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Получение сессии для ДИ."""
    logger.debug("Creating new database session")

    try:
        async with async_session() as session:
            logger.debug("Database session created successfully")
            yield session
            logger.debug("Database session completed successfully")

    except SQLAlchemyError as e:
        logger.error(f"Database session error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in database session: {e}")
        raise
    finally:
        logger.debug("Database session closed")