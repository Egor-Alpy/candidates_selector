import time

from faststream import Depends
from faststream.rabbit import RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.broker import broker, tender_exchange
from app.core.dependencies.services import get_tender_notifier, get_service_es_selector
from app.core.logger import get_logger
from app.core.settings import settings
from app.db.session import get_session
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

        # if not es_candidates or not es_candidates.get("hits", {}).get("hits"):
        #     logger.warning(f"Нет кандидатов для позиции {position.id}")
        #     continue

        # Применяем shrinking к кандидатам
        await shrink_service.shrink(candidates=es_candidates, position=position)

        # Добавить фильтрацию:
        min_points = len(position.attributes) / 2  # type: ignore
        filtered_hits = [
            candidate
            for candidate in es_candidates["hits"]["hits"]
            if candidate.get("points", 0) >= min_points
        ]
        es_candidates["hits"]["hits"] = filtered_hits

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

    # # Сохраняем в файл для отладки
    # with open(f"matching_results_{tender_id}.json", "w", encoding="utf-8") as file:
    #     json.dump(final_results, file, ensure_ascii=False, indent=2)


    tr_es = time.time() - ts_es

    logger.info(60 * '=')
    logger.info(f"Завершен мэтчинг для тендера {tender_id}. Обработано позиций: {len(positions)}")
    logger.info(f'time_pg: {tr_pg} | time_es: {tr_es}')
