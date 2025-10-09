from typing import Optional, List, Dict

from app.core.logger import get_logger
from app.core.settings import settings
from app.db.session import get_session
from app.models.tenders import TenderPositions
from app.repository.postgres import PostgresRepository
from app.services.attrs_standardizer import AttrsStandardizer
from app.services.trigrammer import Trigrammer
from app.services.unit_standardizer import UnitStandardizer
from app.services.vectorizer import SemanticMatcher

from app.services.shrinker.shrinker_positions_service import ShrinkerPositions
from app.services.shrinker.shrinker_products_service import ShrinkerProducts

logger = get_logger(name=__name__)

import asyncio


class Shrinker:
    def __init__(
        self,
    ):
        self.vectorizer = SemanticMatcher()
        self.attrs_sorter = AttrsStandardizer()
        self.unit_normalizer = UnitStandardizer()
        self.trigrammer = Trigrammer()

        self.shrinker_positions = ShrinkerPositions()
        self.shrinker_products = ShrinkerProducts()

        self.semaphore = asyncio.Semaphore(settings.SHRINKER_SEMAPHORE_SIZE)

    async def shrink(self, candidates: dict, position: TenderPositions):
        """Основной метод для оценки кандидатов"""
        try:
            # ЭТАП 1: ПОДГОТОВКА
            logger.info(f"Начало обработки позиции {position.title.upper()}")
            logger.info(f"Присвоенная категория: {position.category}")

            # Парсим атрибуты позиции с группировкой
            position_attrs = await self.shrinker_positions.parse_position_attributes(position.attributes)

            if len(position_attrs.get('attrs', [])) == 0:
                logger.warning("❌ Нет атрибутов для сравнения")
                return


            # ЭТАП 2: ОБРАБОТКА КАНДИДАТОВ
            logger.info(f"🔍 Начинаем обработку {len(candidates['hits']['hits'])} кандидатов")

            position_max_points = len(position.attributes)
            min_required_points = position_max_points * settings.CANDIDATES_TRASHOLD_SCORE
            logger.info(f"Макс. балл: {position_max_points}  | Мин. балл для прохода: {min_required_points}")

            # Создаем tasks для параллельного выполнения
            tasks = [
                self._process_with_semaphore(candidate, position_attrs, min_required_points)
                for candidate in candidates["hits"]["hits"]
            ]
            # Выполняем все tasks параллельно
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Фильтруем успешные результаты
            processed_candidates = [
                result for result in results
                if isinstance(result, dict) and result is not None
            ]

            return processed_candidates

        except Exception as e:
            logger.error(f'Error: {e}')
            return None

    async def _process_with_semaphore(
        self, candidate, position_attrs, min_required_points
    ):
        async with self.semaphore:
            return await self.shrinker_products.process_single_candidate(
                candidate, position_attrs, min_required_points
            )

