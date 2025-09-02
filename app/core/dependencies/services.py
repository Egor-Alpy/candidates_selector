from fastapi import Depends, HTTPException, Request

from app.broker.broker import broker
from app.core.dependencies.repositories import get_es_repository
from app.core.logger import get_logger
from app.repository.elastic import ElasticRepository
from app.services.es_selector import ElasticSearchSelector
from app.services.publisher_service import TenderNotifier
from app.services.shrinker import Shrinker
from app.services.trigrammer import Trigrammer
from app.services.vectorizer import Vectorizer

logger = get_logger(name=__name__)


def get_service_es_selector() -> ElasticSearchSelector:
    """Внедрение зависимости сервиса ElasticSearchSelector"""
    es_repo = get_es_repository()
    logger.debug("⏺️ Создаем экземпляр ElasticSearchSelector сервиса...")
    return ElasticSearchSelector(es_repo=es_repo)



def get_service_trigrammer() -> Trigrammer:
    """Внедрение зависимости сервиса Trigrammer"""
    logger.debug("⏺️ Создаем экземпляр Trigrammer сервиса...")
    return Trigrammer()

def get_service_vectorizer(request: Request) -> Vectorizer:
    vectorizer = getattr(request.app.state, 'vectorizer', None)
    if vectorizer is None:
        raise HTTPException(status_code=500, detail="Векторизатор не инициализирован")
    return vectorizer

async def get_tender_notifier() -> TenderNotifier:
    """Внедрение зависимости сервиса TenderNotifier"""
    return TenderNotifier(broker=broker)  # Передаем нужный broker

def get_service_shrinker(
        trigrammer: Trigrammer = Depends(get_service_trigrammer),
        vectorizer: Vectorizer = Depends(get_service_vectorizer)
) -> Shrinker:
    """Внедрение зависимости сервиса Shrinker"""
    logger.debug("⏺️ Создаем экземпляр Shrinker сервиса...")
    return Shrinker(trigrammer=trigrammer, vectorizer=vectorizer)