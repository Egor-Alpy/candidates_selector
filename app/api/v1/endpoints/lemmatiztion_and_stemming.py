from fastapi import APIRouter, Depends

from app.core.logger import get_logger
from test_scripts.lemmatizator import lemmatizate_and_stemm


router = APIRouter(prefix="/lemmatization_and_stemming", tags=["Handle strings"])
logger = get_logger(name=__name__)


@router.post('/string')
async def process_collection(
        string: str,
):
    """Тестовый локальный эндпоинт для лемматизации и стемминга строки"""
    try:
        return lemmatizate_and_stemm(string)

    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "candidates": [{f"Ошибка api-слоя": {e}}]}
