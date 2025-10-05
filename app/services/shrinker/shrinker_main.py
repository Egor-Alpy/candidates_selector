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
            position_max_points = len(position.attributes)
            min_required_points = position_max_points * settings.CANDIDATES_TRASHOLD_SCORE

            logger.info(
                f'\n'
                f'начало обработки позиции {position.title.upper()}\n'
                f'- Присвоенная категория: {position.category}\n'
                f'- Максимальные баллы: {position_max_points}\n'
                f'- Минимум для прохода: {min_required_points}'
            )

            # Парсим атрибуты позиции с группировкой
            position_attrs = await self.shrinker_positions.parse_position_attributes(position.attributes)

            if len(position_attrs.get('attrs', [])) == 0:
                logger.warning("❌ Нет атрибутов для сравнения")
                return


            # ЭТАП 2: ОБРАБОТКА КАНДИДАТОВ
            logger.info(f"🔍 Начинаем обработку {len(candidates['hits']['hits'])} кандидатов")

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


            # ЭТАП 3: ФИНАЛЬНАЯ ОБРАБОТКА
            await self._finalize_results(
                candidates,
                processed_candidates,
                position,
                min_required_points
            )
        except Exception as e:
            logger.error(f'Error: {e}')

    async def _process_with_semaphore(
        self, candidate, position_attrs, min_required_points
    ):
        async with self.semaphore:
            return await self.shrinker_products.process_single_candidate(
                candidate, position_attrs, min_required_points
            )

    async def _finalize_results(
        self,
        candidates: dict,
        processed_candidates: List[Dict],
        position: TenderPositions,
        min_required_points: int,
    ):
        """Финальная обработка результатов"""
        try:
            processed_candidates.sort(key=lambda x: x["points"], reverse=True)

            candidates["hits"]["hits"] = [
                item["candidate"] for item in processed_candidates
            ]

            attributes_matches_data = []
            tender_matches_data = []

            for i, result in enumerate(processed_candidates):
                tender_position_id = position.id
                logger.warning(result)
                tender_position_max_points = len(position.attributes)
                tender_position_score = result.get("points")
                tender_position_percentage_match_score = round(tender_position_score / tender_position_max_points * 100, 1)
                product_mongo_id = result['candidate']['_source']['id']

                # Данные для основного соответствия
                tender_match_data = {
                    "tender_position_id": tender_position_id,
                    "product_id": product_mongo_id,
                    "match_score": tender_position_score,
                    "max_match_score": tender_position_max_points,
                    "percentage_match_score": tender_position_percentage_match_score,
                }
                tender_matches_data.append(tender_match_data)

                for matched_char in result['matched_attributes']:
                    match_data = {
                        'tender_id': position.tender_id,
                        'tender_position_id': tender_position_id,
                        'product_mongo_id': product_mongo_id,
                        'position_attr_id': matched_char['position_attr_id'],
                        'position_attr_name': matched_char['original_position_attr_name'],
                        'position_attr_value': matched_char['original_position_attr_value'],
                        'position_attr_unit': matched_char.get('original_position_attr_unit'),
                        'product_attr_name': matched_char['original_product_attr_name'],
                        'product_attr_value': str(matched_char['original_product_attr_value']),
                    }
                    attributes_matches_data.append(match_data)
            logger.info(f'Position {position.title} has been handled! Products matches: {len(processed_candidates)}')

            async for fresh_session in get_session():
                try:
                    fresh_pg_service = PostgresRepository(fresh_session)

                    await fresh_pg_service.increment_processed_positions(
                        tender_id=position.tender_id
                    )

                    if tender_matches_data:
                        await fresh_pg_service.create_tender_matches_batch(
                            tender_matches_data
                        )

                    if attributes_matches_data:
                        await fresh_pg_service.create_tender_position_attribute_matches_bulk(
                            attributes_matches_data
                        )


                except Exception as e:
                    logger.error(f"Database operation failed: {e}")
                    await fresh_session.rollback()
                    raise

            # Создаем расширенный отчет

            # report_filename = f"shrinking_report_{position.id}_{int(time.time())}.json"
            # with open(report_filename, "w", encoding="utf-8") as f:
            #     json.dump(report, f, ensure_ascii=False, indent=2)  # Todo: dev env only!

            # logger.info(f"📄 Отчет сохранен: {report_filename}")
        except Exception as e:
            logger.error(e)

    def _analyze_attribute_types(self, processed_candidates: List[Dict]) -> Dict:
        """Анализ эффективности матчинга по типам атрибутов"""
        type_analysis = {
            "boolean": {"total_matches": 0, "successful_matches": 0},
            "numeric": {"total_matches": 0, "successful_matches": 0},
            "string": {"total_matches": 0, "successful_matches": 0},
            "range": {"total_matches": 0, "successful_matches": 0},
            "multiple": {"total_matches": 0, "successful_matches": 0},
        }

        for candidate in processed_candidates:
            for match in candidate.get("matched_attributes", []):
                pos_type = match.get("position_attr_type", "unknown")
                if pos_type in type_analysis:
                    type_analysis[pos_type]["successful_matches"] += 1

        return type_analysis
