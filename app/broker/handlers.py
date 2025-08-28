import json

from faststream import Depends
from faststream.rabbit import RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.broker import broker, tender_exchange
from app.core.dependencies.repositories import get_postgres_repository
from app.core.dependencies.services import get_tender_notifier, get_service_es_selector, get_service_shrinker
from app.core.logger import get_logger
from app.db.session import get_session
from app.repository.postgres import PostgresRepository
from app.services.es_selector import ElasticSearchSelector
from app.services.publisher_service import TenderNotifier
from app.services.shrinker import Shrinker

logger = get_logger(name=__name__)


@broker.subscriber(
    RabbitQueue("matching_queue", durable=True, routing_key="tender.ready_for_matching"),
    tender_exchange,
)
async def handle_tender_categorization(
    tender_id: int,
    tender_number: str = None,
    customer_name: str = None,
    notifier: TenderNotifier = Depends(get_tender_notifier),
    es_service: ElasticSearchSelector = Depends(get_service_es_selector),
    shrink_service: Shrinker = Depends(get_service_shrinker),
    session: AsyncSession = Depends(get_session),
):
    logger.info(f"Получен тендер для мэтчинга: {tender_id}")

    pg_service = PostgresRepository(session)

    positions = await pg_service.get_tender_positions_selectinload(tender_id)

    candidates = {}
    for position in positions:
        es_candidates = await es_service.find_candidates_for_rabbit(
            index_name="products_v5",
            position=position
        )
        shrinked_candidates = shrink_service.shrink(candidates=candidates, position=position)

    # logger.info(candidates)
    return candidates
