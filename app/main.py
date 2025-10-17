from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import router
from app.broker.broker import broker
from app.core.logger import get_logger
from app.core.settings import settings

from app.core.connection_pool import connection_pool
import app.broker.handlers

logger = get_logger(name=__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    logger.info(f"🚀 Запуск {settings.PROJECT_NAME} сервиса...")
    logger.info(f"⚡️ Режим: {settings.ENV_MODE.upper()}")
    logger.info(f'📝 Уровень логирования: {settings.LOG_LEVEL}')

    if settings.is_production_mode:
        await broker.start()
        logger.info("✅ RabbitMQ consumer запущен!")
    else:
        logger.warning("⚠️  RabbitMQ consumer ОТКЛЮЧЕН!")
        logger.warning("💡 Используйте /api/v1/tender_test для тестирования")

    yield

    # Shutdown
    logger.info(f"🛑 Остановка {settings.PROJECT_NAME}")

    await broker.close()
    await connection_pool.close_all()  # Добавить эту строку

    logger.info("✅ Все соединения закрыты")


# Создание FastAPI приложения
app = FastAPI(
    title=f"{settings.PROJECT_NAME} API",
    description=settings.PROJECT_DESCRIPTION,
    version=settings.PROJECT_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
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
