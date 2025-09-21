import json
from typing import Optional, Dict, Any, List

from app.core.logger import get_logger
from app.core.settings import settings
from app.core.connection_pool import connection_pool

logger = get_logger(name=__name__)


class ElasticRepository:
    """Репозиторий для работы с Elasticsearch с пулом соединений"""

    def __init__(self):
        # Убираем создание клиента - используем пул соединений
        pass

    async def _get_client(self):
        """Получение клиента Elasticsearch из пула соединений"""
        return await connection_pool.get_es_client()

    async def disconnect(self):
        """Закрытие подключения - теперь управляется пулом"""
        logger.info("🔌 Elasticsearch disconnect called (managed by connection pool)")

    async def is_connected(self) -> bool:
        """Проверка подключения"""
        try:
            client = await self._get_client()
            await client.info()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка проверки подключения: {e}")
            return False

    async def insert_document(self, index_name: str, document: Dict[str, Any]) -> bool:
        """Индексация документа"""
        try:
            doc_id = document.get('title')
            client = await self._get_client()

            response = await client.index(
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
            client = await self._get_client()

            # Формируем тело запроса
            body = {
                "query": query or {"match_all": {}},
                "size": size,
                "from": from_
            }

            if sort:
                body["sort"] = sort

            response = await client.search(
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
            field="indexed_at"
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
            client = await self._get_client()
            return await client.indices.exists(index=index_name)

        except Exception as e:
            logger.error(f"❌ Error checking index existence {index_name}: {e}")
            return False

    async def create_index(self, index_name: str, body: Dict[str, Any] = None) -> bool:
        """Создание индекса с маппингом"""
        try:
            client = await self._get_client()

            # Проверяем, существует ли индекс
            if await self.index_exists(index_name):
                logger.info(f"📋 Index {index_name} already exists")
                return True

            await client.indices.create(
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
            client = await self._get_client()
            response = await client.count(index=index_name)
            return response.get("count", 0)

        except Exception as e:
            logger.error(f"❌ Error getting document count for {index_name}: {e}")
            return 0

    async def make_query(self, index_name: str, body: dict):
        """Сделать простой запрос в эластик"""
        try:
            logger.debug(f"🔍 Index: {index_name}")
            logger.debug(
                f"🔍 Query body: {json.dumps(body, ensure_ascii=False, indent=2)}"
            )

            client = await self._get_client()
            response = await client.search(index=index_name, body=body)

            total_hits = response.body["hits"]["total"]
            logger.debug(f"📊 Total hits: {total_hits}")
            logger.debug(f"📊 Returned docs: {len(response.body['hits']['hits'])}")

            return response.body

        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return False