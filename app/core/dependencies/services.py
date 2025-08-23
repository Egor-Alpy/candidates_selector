from fastapi import Depends

from app.core.dependencies.repositories import get_es_repository
from app.core.logger import get_logger
from app.repository.elastic import ElasticRepository
from app.services.es_selector import ElasticSearchSelector

logger = get_logger(name=__name__)


def get_service_es_selector(
        es_repo: ElasticRepository = Depends(get_es_repository)
) -> ElasticSearchSelector:
    """Внедрение зависимости сервиса ElasticSearchSelector"""
    logger.debug("⏺️ Создаем экземпляр ElasticSearchSelector сервиса...")
    return ElasticSearchSelector(es_repo=es_repo)


def get_service_vector_selector() -> None:
    """Внедрение зависимости сервиса VectorSelector"""
    logger.debug("⏺️ Создаем экземпляр VectorSelector сервиса...")
    return None
