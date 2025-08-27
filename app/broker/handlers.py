
from faststream import Depends
from faststream.rabbit import RabbitQueue

from app.broker.broker import broker, tender_exchange
from app.core.dependencies.services import get_tender_notifier
from app.core.logger import get_logger
from app.services.publisher_service import TenderNotifier

logger = get_logger(name=__name__)


@broker.subscriber(
    RabbitQueue("matching_queue", durable=True, routing_key="tender.ready_for_matching"),
    tender_exchange,
)
async def handle_tender_categorization(
    tender_id: int,
    tender_number: str = None,
    customer_name: str = None,
    notifier: TenderNotifier = Depends(get_tender_notifier)
):
    logger.info(f"Получен тендер для мэтчинга: {tender_id}")

    #ToDo логика тут

    logger.info(f'Галя нам п{tender_id}зда')