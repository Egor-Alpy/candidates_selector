import asyncio
import logging
from typing import Dict, Optional
import aiohttp
from elasticsearch import AsyncElasticsearch
from app.core.settings import settings

logger = logging.getLogger(__name__)


class SimpleConnectionPool:
    """Простой пул соединений для продакшна"""

    def __init__(self):
        self._http_sessions: Dict[str, aiohttp.ClientSession] = {}
        self._es_client: Optional[AsyncElasticsearch] = None
        self._lock = asyncio.Lock()

    async def get_http_session(self, service_name: str) -> aiohttp.ClientSession:
        """Получить HTTP сессию для сервиса"""
        async with self._lock:
            if (
                service_name not in self._http_sessions
                or self._http_sessions[service_name].closed
            ):

                connector = aiohttp.TCPConnector(
                    limit=30,  # Максимум соединений
                    limit_per_host=15,  # На хост
                    keepalive_timeout=60,
                    enable_cleanup_closed=True,
                )

                session = aiohttp.ClientSession(
                    connector=connector, timeout=aiohttp.ClientTimeout(total=30)
                )

                self._http_sessions[service_name] = session
                logger.info(f"✅ Created HTTP session for {service_name}")

            return self._http_sessions[service_name]

    async def get_es_client(self) -> AsyncElasticsearch:
        """Получить Elasticsearch клиент"""
        async with self._lock:
            if self._es_client is None:
                self._es_client = AsyncElasticsearch(
                    hosts=[settings.get_elastic_dsn],
                    max_retries=settings.ES_MAX_RETRIES,
                    retry_on_timeout=True,
                    timeout=30,
                    maxsize=25,
                )
                logger.info("✅ Created Elasticsearch client")

            return self._es_client

    async def close_all(self):
        """Закрыть все соединения"""
        logger.info("🔌 Closing all connections...")

        for service_name, session in self._http_sessions.items():
            if not session.closed:
                await session.close()

        if self._es_client:
            await self._es_client.close()

        self._http_sessions.clear()
        self._es_client = None
        logger.info("✅ All connections closed")


# Глобальный экземпляр
connection_pool = SimpleConnectionPool()
