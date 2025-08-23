from contextlib import asynccontextmanager

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from app.api.router import router
from app.core.settings import settings

from app.core.logger import get_logger
from app.services.absorber import Absorber

logger = get_logger(name=__name__)



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""

    logger.info("🚀 Запуск API сервера MongoAbsorber")


    yield

    # Shutdown
    logger.info("🛑 Остановка API сервера")



# Создание FastAPI приложения
app = FastAPI(
    title="MongoAbsorber API",
    description="API для управления и мониторинга MongoAbsorber - системы синхронизации MongoDB → Elasticsearch",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="info"
    )

