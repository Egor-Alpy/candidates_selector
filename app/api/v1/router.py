from fastapi import APIRouter

from app.api.v1.endpoints import select

api_router = APIRouter(prefix='/v1')

routers = [select.router]

# Подключаем роутеры
for router in routers:
    api_router.include_router(
        router,
    )
