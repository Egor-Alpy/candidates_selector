from fastapi import APIRouter, Depends

from app.core.dependencies.services import get_service_trigrammer, get_service_vectorizer
from app.core.logger import get_logger
from app.services.trigrammer import Trigrammer
from app.services.vectorizer import Vectorizer

router = APIRouter(prefix="/compare", tags=["Select Candidates"])
logger = get_logger(name=__name__)


@router.post('/strings')
async def process_collection(
        string1: str,
        string2: str,
        trigrammer: Trigrammer = Depends(get_service_trigrammer),
        vectorizer: Vectorizer = Depends(get_service_vectorizer)
):
    """Получение кандидатов из коллекции индекса ES"""
    try:
        ngram_similarity = await trigrammer.compare_two_strings(string1, string2)
        vector_similarity = await vectorizer.compare_two_strings(string1, string2)

        return {
            "similarity": ngram_similarity + vector_similarity,
            "ngram_similarity": ngram_similarity,
            "vector_similarity": vector_similarity
        }
    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "candidates": [{f"Ошибка api-слоя": {e}}]}

