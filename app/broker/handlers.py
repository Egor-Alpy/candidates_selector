import time
from typing import Dict, List

from faststream import Depends
from faststream.rabbit import RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.broker import broker, tender_exchange
from app.core.dependencies.services import get_tender_notifier, get_service_es_selector
from app.core.logger import get_logger
from app.core.settings import settings
from app.db.session import get_session
from app.models.tenders import TenderPositions
from app.repository.postgres import PostgresRepository
from app.services.es_selector import ElasticSelector
from app.services.publisher_service import TenderNotifier
from app.services.shrinker.shrinker_main import Shrinker

logger = get_logger(name=__name__)


@broker.subscriber(
    RabbitQueue(
        "matching_queue", durable=True, routing_key="tender.categorized"
    ),
    tender_exchange,
)
async def handle_tender_categorization(
    tender_id: int,
    tender_number=None,
    customer_name=None,
    notifier: TenderNotifier = Depends(get_tender_notifier),
    es_service: ElasticSelector = Depends(get_service_es_selector),
    session: AsyncSession = Depends(get_session),
):
    # Исправлено: передаем все зависимости в shrink_service
    shrink_service = Shrinker()

    logger.info(f"Получен тендер для мэтчинга: {tender_id}")

    ts_pg = time.time()

    pg_service = PostgresRepository(session)
    positions = await pg_service.get_tender_positions_selectinload(tender_id)
    company_id: str = await pg_service.get_company_id_by_tender(tender_id)

    logger.info(f"TENDER_ID: {tender_id} | COMPANY_ID: {company_id}")

    tr_pg = time.time() - ts_pg

    # Исправлено: накапливаем результаты по всем позициям
    all_position_results = []

    ts_es = time.time()

    logger.info(f'для обработки пришло позиций: {len(positions)}')

    for position in positions:

        # Получаем кандидатов для позиции
        es_candidates = await es_service.find_candidates_for_rabbit(
            index_name=settings.ES_INDEX, position=position
        )

        # Применяем shrinking к кандидатам
        processed_candidates = await shrink_service.shrink(candidates=es_candidates, position=position)

        # ЭТАП 3: ФИНАЛЬНАЯ ОБРАБОТКА
        await _finalize_results(candidates=es_candidates, processed_candidates=processed_candidates, position=position)

        # Сохраняем результаты для позиции
        position_result = {
            "position_id": position.id,
            "position_title": position.title,
            "candidates_count": len(es_candidates["hits"]["hits"]),
            "candidates": es_candidates["hits"]["hits"],
        }
        all_position_results.append(position_result)

    # Сохраняем все результаты
    final_results = {
        "tender_id": tender_id,
        "tender_number": tender_number,
        "customer_name": customer_name,
        "positions_count": len(positions),
        "results": all_position_results,
    }

    tr_es = time.time() - ts_es

    logger.info(f"Завершен мэтчинг для тендера {tender_id}. Обработано позиций: {len(positions)}")
    logger.info(f'операции с PG: {round(tr_pg, 2)} сек. | мэтчер: {round(tr_es, 2)} сек.')
    logger.info(f"{60 * '='}\n")


async def _finalize_results(
    candidates: dict,
    processed_candidates: List[Dict],
    position: TenderPositions
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
        async for fresh_session in get_session():
            try:
                fresh_pg_service = PostgresRepository(fresh_session)
                position_number = await fresh_pg_service.increment_processed_positions(
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

            logger.info(f"[№{position_number}] ✅ Позиция '{position.title}' обработана! Подобрано {len(processed_candidates)} товаров.\n")
        # Создаем расширенный отчет
        # report_filename = f"shrinking_report_{position.id}_{int(time.time())}.json"
        # with open(report_filename, "w", encoding="utf-8") as f:
        #     json.dump(report, f, ensure_ascii=False, indent=2)  # Todo: dev env only!
        # logger.info(f"📄 Отчет сохранен: {report_filename}")
    except Exception as e:
        logger.error(e)
