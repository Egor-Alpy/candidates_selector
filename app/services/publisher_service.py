import logging
from typing import Optional

from faststream.rabbit import RabbitBroker

from app.broker.broker import tender_exchange
from app.schemas.messages import TenderCreatedMessage

logger = logging.getLogger(__name__)

class TenderNotifier:
    """Сервис для отправки событий о тендерах через RabbitMQ"""
    def __init__(self, broker: RabbitBroker):
        self.broker = broker

    async def send_tender_event(
            self,
            tender_id: int,
            tender_number: Optional[str] = None,
            customer_name: Optional[str] = None
    ):
        """Отправляет событие о сохранении тендера в очередь tender_ids"""
        logger.info(f"🐰 Подготовка отправки события tender_saved для тендера {tender_id}")

        try:
            message = TenderCreatedMessage(
                tender_id=tender_id,
                tender_number=tender_number,
                customer_name=customer_name
            )

            message_dict = message.model_dump()
            logger.debug(f"📨 Сообщение подготовлено: {message_dict}")

            # Отправляем сообщение в очередь
            logger.debug(f"📤 Отправка в очередь tender_ids через broker...")
            await self.broker.publish(
                message_dict,
                exchange=tender_exchange,
                routing_key="tender.ready_for_categorization"
                #ToDo Определить routing_key
            )

            logger.info(
                f"✅ Событие tender_saved успешно отправлено: tender_id={tender_id}, tender_number={tender_number}")

        except Exception as e:
            logger.error(f"❌ Ошибка отправки события tender_saved: {e}", exc_info=True)
            logger.error(f"🔍 Детали ошибки - tender_id: {tender_id}, tender_number: {tender_number}")
            # Не падаем, если RabbitMQ недоступен - это не критично для основного процесса
            logger.warning(f"⚠️ RabbitMQ событие не отправлено, но тендер {tender_id} сохранен в БД")
