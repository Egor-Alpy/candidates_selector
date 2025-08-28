from fastapi import APIRouter, Depends

from app.core.dependencies.services import get_service_es_selector
from app.core.es_settings import ElasticQueries
from app.core.logger import get_logger
from app.schemas.request import ESCandidatesRequest
from app.schemas.response import ESCandidatesResponse
from app.services.es_selector import ElasticSearchSelector

router = APIRouter(prefix="/select", tags=["Select Candidates"])
logger = get_logger(name=__name__)


@router.post('/es_1', response_model=ESCandidatesResponse)
async def process_collection(
        request: ESCandidatesRequest,
        es_service: ElasticSearchSelector = Depends(get_service_es_selector),
):
    """Получение кандидатов из коллекции индекса ES"""
    try:
        body = ElasticQueries.get_query_v1(
            position_title=request.position_title,
            yandex_category=request.position_yandex_category,
            size=request.size
        )
        candidates = await es_service.find_candidates(
            index_name=request.index_name,
            body=body
        )
        return {"status": True, "candidates": candidates}
    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "candidates": [{f"Ошибка api-слоя": {e}}]}


