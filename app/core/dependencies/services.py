from app.core.logger import get_logger
from app.services.es_selector import ElasticSearchSelector
from fastapi import Depends

logger = get_logger(name=__name__)


def es_service_es_selector() -> ElasticSearchSelector:
    """Внедрение зависимости сервиса ElasticSearchSelector"""
    logger.debug("⏺️ Создаем экземпляр ElasticSearchSelector сервиса...")
    return ElasticSearchSelector()


