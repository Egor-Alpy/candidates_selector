from app.core.logger import get_logger
from app.repository.elastic import ElasticRepository
from app.repository.mongo import MongoRepository

logger = get_logger(name=__name__)


def get_es_repository() -> ElasticRepository:
    """Внедрение зависимости сервиса ElasticRepository"""
    logger.debug("🔼 Создаем экземпляр ElasticRepository репозитория...")
    return ElasticRepository()


def get_mongo_repository() -> MongoRepository:
    """Внедрение зависимости сервиса MongoRepository"""
    logger.debug("🔼 Создаем экземпляр MongoRepository репозитория...")
    return MongoRepository()
