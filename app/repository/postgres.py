from typing import Optional, List, Sequence, Dict, Any, Union

from fastapi import Depends
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select, text

from app.core.logger import get_logger
from app.models.tenders import (
    TenderPositions,
    TenderPositionAttributesMatches,
    Matches,
    TenderInfo,
)

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

    async def get_tender_positions_selectinload(self, tender_id: int) -> Sequence[TenderPositions] | None:
        """Получение позиций тендера по его pg id вместе с атрибутами"""
        try:
            stmt = (
                select(TenderPositions)
                .options(selectinload(TenderPositions.attributes))
                .where(TenderPositions.tender_id == tender_id)
                .order_by(TenderPositions.tender_position.asc().nulls_last())
            )

            result = await self.db.execute(stmt)
            positions = result.scalars().all()

            return positions

        except Exception as e:
            logger.error(f"Ошибка получения позиций тендера {tender_id}: {e}")
            return None

    async def get_company_id_by_tender(self, tender_id: int) -> Optional[str]:
        """Получение company_id по tender_id"""
        try:
            stmt = select(TenderInfo.company_id).where(TenderInfo.id == tender_id)
            result = await self.db.execute(stmt)
            company_id = result.scalar_one_or_none()

            return company_id

        except Exception as e:
            logger.error(f"Ошибка получения company_id для тендера {tender_id}: {e}")
            return None

    async def create_tender_position_attribute_matches_bulk(
        self, matches_data: List[Dict[str, Any]]
    ) -> int | None:
        """Массовая вставка соответствий атрибутов позиций тендера"""
        try:
            if not matches_data:
                logger.warning("Пустой список данных для массовой вставки атрибутов")
                return 0

            stmt = insert(TenderPositionAttributesMatches).values(matches_data)
            result = await self.db.execute(stmt)
            await self.db.commit()

            rowcount = result.rowcount
            return True

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Ошибка массового создания соответствий атрибутов: {e}")
            return None

    async def create_tender_match(
        self, tender_position_id: int, product_id: int, match_score: int, max_match_score: int, percentage_match_score: float
    ) -> Matches | None:
        """Создание одного соответствия тендера"""
        try:
            new_match = Matches(
                tender_position_id=tender_position_id,
                product_id=product_id,
                match_score=match_score,
                max_match_score=max_match_score,
                percentage_match_score=percentage_match_score
            )

            self.db.add(new_match)
            await self.db.commit()
            await self.db.refresh(new_match)

            return new_match

        except Exception as e:
            await self.db.rollback()
            logger.error(
                f"Ошибка создания соответствия для позиции {tender_position_id}, продукта {product_id}: {e}"
            )
            return None

    async def create_tender_matches_batch(
        self, matches_data: List[Dict]
    ) -> List[Matches] | None:
        """Батчевое создание соответствий тендера"""
        if not matches_data:
            return []

        try:
            # Создаем объекты для вставки
            matches_objects = [
                Matches(
                    tender_position_id=match_data.get("tender_position_id"),
                    product_id=match_data.get("product_id"),
                    match_score=match_data.get("match_score"),
                    max_match_score=match_data.get("max_match_score"),
                    percentage_match_score=match_data.get("percentage_match_score"),
                )
                for match_data in matches_data
            ]

            # Добавляем все объекты в сессию
            self.db.add_all(matches_objects)
            await self.db.commit()

            return matches_objects

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Ошибка батчевого создания соответствий тендера: {e}")
            return None

    async def increment_processed_positions(self, tender_id: int) -> Union[int, None]:
        """Увеличивает поле processed_positions на 1 для указанного тендера"""
        try:
            stmt = (
                update(TenderInfo)
                .where(TenderInfo.id == tender_id)
                .values(processed_positions=TenderInfo.processed_positions + 1)
                .returning(TenderInfo.processed_positions)
            )
            result = await self.db.execute(stmt)
            await self.db.commit()

            new_value = result.scalar()
            return new_value

        except Exception as e:
            await self.db.rollback()
            logger.error(
                f"Ошибка увеличения processed_positions для тендера {tender_id}: {e}"
            )
            return None
