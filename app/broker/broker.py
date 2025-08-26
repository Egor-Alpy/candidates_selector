import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, ExchangeType

from app.core.settings import settings

logger = logging.getLogger(__name__)

# Создание брокера
broker = RabbitBroker(url=settings.get_rabbitmq_dsn)
app = FastStream(broker)

# Exchange для событий о тендерах
tender_exchange = RabbitExchange(
    name="tender.events",
    type=ExchangeType.TOPIC,
    durable=True
)