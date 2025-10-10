from fastapi import APIRouter, Depends

from app.core.logger import get_logger
from test_scripts.lemmatizator import lemmatizate_and_stemm


router = APIRouter(prefix="/lemmatization_and_stemming", tags=["Lemmatization And Stemming"])
logger = get_logger(name=__name__)


@router.post('/string')
async def string(
        string: str,
):
    """Тестовый локальный эндпоинт для лемматизации и стемминга строки"""
    try:
        return lemmatizate_and_stemm(string)

    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "candidates": [{f"Ошибка api-слоя": {e}}]}


@router.post("/compare_strings")
async def compare_strings(
    string1: str,
    string2: str,
):
    """Тестовый локальный эндпоинт для лемматизации и стемминга строки"""
    try:
        string1_handled = lemmatizate_and_stemm(string1)
        string2_handled = lemmatizate_and_stemm(string2)

        return {
            "lemma_equally": string1_handled["lemma"] == string2_handled["lemma"],
            "stem_equally": string1_handled["stem"] == string2_handled["stem"],
            "full_data": {"string1": string1_handled, "string2": string2_handled},
        }

    except Exception as e:
        logger.error(f"Ошибка api-слоя: {e}")
        return {"status": False, "error": str(e)}
