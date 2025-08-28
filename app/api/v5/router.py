from fastapi import APIRouter

from app.api.v5.endpoints import select

api_router = APIRouter(prefix='/v5', tags=['v5'])

routers = [select.router]

# Подключаем роутеры
for router in routers:
    api_router.include_router(
        router,
    )
