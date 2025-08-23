from typing import Optional, Dict, Any, List

from elasticsearch import AsyncElasticsearch

from app.core.logger import get_logger
from app.core.settings import settings

logger = get_logger(name=__name__)


class ElasticRepository:
    """Расширенный репозиторий для работы с Elasticsearch"""

    def __init__(self):
        self.client: Optional[AsyncElasticsearch] = self._get_client()

    def _get_client(self) -> Optional[AsyncElasticsearch]:
        """Подключение к Elasticsearch"""
        try:
            client = AsyncElasticsearch(
                hosts=[settings.get_elastic_dsn],
                max_retries=settings.ABSORBER_MAX_RETRIES,
                retry_on_timeout=True
            )

            logger.info("✅ Подключение к Elasticsearch установлено")
            return client

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Elasticsearch: {e}")
            return None

    def _connect_to_db(self):
        """Подключение к бд"""
        if not self.is_connected():
            logger.error(f"❌ Ошибка подключения к БД MongoDB: {self.client} is not connected!")
            return False
        try:
            database = self.client[settings.MONGO_DB_NAME]
            logger.info(f"✅ Подключение к MongoDB установлено: {settings.MONGO_DB_NAME}")
            return database

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД MongoDB: {e}")
            return None

    async def disconnect(self):
        """Закрытие подключения"""
        if self.client:
            try:
                await self.client.close()
                logger.info("🔌 Подключение к Elasticsearch закрыто")
            except Exception as e:
                logger.error(f"❌ Ошибка при закрытии подключения: {e}")

    async def is_connected(self) -> bool:
        """Проверка подключения"""
        if not self.client:
            return False
        try:
            await self.client.info()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка проверки подключения: {e}")
            return False

    async def insert_document(self, index_name: str, document: Dict[str, Any]) -> bool:
        """Индексация документа"""
        try:
            doc_id = document.get('title')
            if not self.client:
                logger.error("❌ Elasticsearch client не инициализирован")
                return False

            response = await self.client.index(
                index=index_name,
                body=document
            )

            # Проверяем результат операции
            if response.get("result") in ["created", "updated"]:
                logger.debug(f"✅ Document {doc_id} indexed successfully")
                return True
            else:
                logger.warning(f"⚠️ Unexpected response for document {doc_id}: {response}")
                return False

        except Exception as e:
            logger.error(f"❌ Error indexing document {doc_id}: {e}")
            return False

    async def search_documents(
            self,
            index_name: str,
            query: Dict[str, Any] = None,
            sort: List[Dict[str, Any]] = None,
            size: int = 10,
            from_: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Поиск документов с возможностью сортировки"""
        try:
            if not self.client:
                logger.error("❌ Elasticsearch client не инициализирован")
                return None

            # Формируем тело запроса
            body = {
                "query": query or {"match_all": {}},
                "size": size,
                "from": from_
            }

            if sort:
                body["sort"] = sort

            response = await self.client.search(
                index=index_name,
                body=body
            )

            return response

        except Exception as e:
            logger.error(f"❌ Error searching in index {index_name}: {e}")
            return None

    async def get_last_document_by_field(
            self,
            index_name: str,
            field: str = "indexed_at"  # ← Изменили по умолчанию
    ) -> Optional[Dict[str, Any]]:
        """Получение последнего документа по указанному полю"""
        try:
            # Если пытаются сортировать по _id, используем indexed_at
            if field == "_id":
                field = "indexed_at"
                logger.warning("⚠️ Сортировка по _id не поддерживается, используем indexed_at")

            # Поиск с сортировкой по убыванию
            result = await self.search_documents(
                index_name=index_name,
                query={"match_all": {}},
                sort=[{field: {"order": "desc"}}],
                size=1
            )

            if result and result["hits"]["hits"]:
                hit = result["hits"]["hits"][0]
                doc = hit["_source"]
                doc["_id"] = hit["_id"]  # Добавляем _id в документ
                return doc

            return None

        except Exception as e:
            logger.error(f"❌ Error getting last document by {field}: {e}")
            return None

    async def index_exists(self, index_name: str) -> bool:
        """Проверка существования индекса"""
        try:
            if not self.client:
                return False

            return await self.client.indices.exists(index=index_name)

        except Exception as e:
            logger.error(f"❌ Error checking index existence {index_name}: {e}")
            return False

    async def create_index(self, index_name: str, body: Dict[str, Any] = None) -> bool:
        """Создание индекса с маппингом"""
        try:
            if not self.client:
                return False

            # Проверяем, существует ли индекс
            if await self.index_exists(index_name):
                logger.info(f"📋 Index {index_name} already exists")
                return True

            await self.client.indices.create(
                index=index_name,
                body=body
            )

            logger.info(f"✅ Index {index_name} created successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Error creating index {index_name}: {e}")
            return False

    async def get_document_count(self, index_name: str) -> int:
        """Получение количества документов в индексе"""
        try:
            if not self.client:
                return 0

            response = await self.client.count(index=index_name)
            return response.get("count", 0)

        except Exception as e:
            logger.error(f"❌ Error getting document count for {index_name}: {e}")
            return 0


# Создание глобального экземпляра
es_repository = ElasticRepository()
