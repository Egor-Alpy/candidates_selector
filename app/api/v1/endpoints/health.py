import logging
from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger("document_processor_service")


@router.get("/healthz")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy"}