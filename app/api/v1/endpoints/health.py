import logging
from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy"}