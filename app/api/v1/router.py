from fastapi import APIRouter

from app.api.v1.endpoints import select, compare

api_router = APIRouter(prefix='/v1', tags=['v1'])

routers = [select.router, compare.router]

# Подключаем роутеры
for router in routers:
    api_router.include_router(
        router,
    )
