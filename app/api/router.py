from fastapi import APIRouter

from app.api.v1 import router as v1_router

router = APIRouter()

@router.get("/", summary="Корневой эндпоинт")
async def root():
    """Корневой эндпоинт API"""
    return {
        "service": "MongoAbsorber API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

router.include_router(v1_router.api_router)