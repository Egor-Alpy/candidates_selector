from typing import Optional, List

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, text

from app.core.logger import get_logger
from app.db.session import get_session
from app.models.tenders import TenderPositions

logger = get_logger(name=__name__)

class PostgresRepository:
    def __init__(self, session: AsyncSession):
        self.db = session

    async def get_tender_positions(self, tender_id: int) -> List[dict] | None:
        """Получение позиций тендера по его pg id"""
        try:
            stmt = select(TenderPositions.id, TenderPositions.title, TenderPositions.category).where(TenderPositions.tender_id == tender_id)
            result = await self.db.execute(stmt)
            data_dict = [dict(row) for row in result.mappings()]

            return data_dict

        except Exception as e:
            logger.error(f'Ошибка получения позиций тендера {tender_id}: {e}')
            return None