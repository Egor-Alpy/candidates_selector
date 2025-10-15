from fastapi import APIRouter

from app.api.v1.endpoints import health, compare, tenders_test

api_router = APIRouter(prefix='/v1')

routers = [health.router, compare.router, tenders_test.router]

# Подключаем роутеры
for router in routers:
    api_router.include_router(
        router,
    )
