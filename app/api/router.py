from fastapi import APIRouter

from app.api.v1 import router as v1_router

# Общий главный роутер
router = APIRouter()

# Роутер для корневой ручки
router_root = APIRouter(tags=['Root'])
@router_root.get("/", summary="Корневой эндпоинт")
async def root():
    """Корневой эндпоинт API"""
    return {
        "service": "MongoAbsorber API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }

# роутер для подключения всех ручек vN_api
router_api = APIRouter(prefix="/api")
router_api.include_router(v1_router.api_router)


router.include_router(router_api)
router.include_router(router_root)
