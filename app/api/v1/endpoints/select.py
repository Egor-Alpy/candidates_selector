from fastapi import APIRouter, Depends

from app.core.dependencies.services import get_service_es_selector, get_service_vector_selector
from app.core.logger import get_logger
from app.schemas.request import ESCandidatesRequest, VectorCandidatesRequest
from app.schemas.response import ESCandidatesResponse, VectorCandidatesResponse
from app.services.es_selector import ElasticSearchSelector
from app.services.vector_selector import VectorSelector

router = APIRouter(prefix="/select", tags=["Select Candidates"])
logger = get_logger(name=__name__)


# @router.post('/vector', response_model=VectorCandidatesResponse)
# async def process_collection(
#         request: VectorCandidatesRequest,
#         processing_service: VectorSelector = Depends(get_service_vector_selector),
# ):
#     """Получение кандидатов по результату сравнения вектора названия позиции с векторами названий товаров из БД"""
#     try:
#         return {"status": True, "candidates": [{}, {}]}
#     except Exception as e:
#         logger.error(f"Ошибка api-слоя: {e}")
#         return {"status": False, "candidates": [{}, {}]}


@router.post('/es_1', response_model=ESCandidatesResponse)
async def process_collection(
        request: ESCandidatesRequest,
        processing_service: ElasticSearchSelector = Depends(get_service_es_selector),
):
    """Получение кандидатов из коллекции индекса ES"""
    try:
        return {"status": True, "candidates": [{}, {}]}
    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "candidates": [{}, {}]}
