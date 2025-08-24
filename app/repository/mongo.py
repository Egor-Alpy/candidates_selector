from typing import Optional, Dict, Any, List

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

from app.core.logger import get_logger
from app.core.settings import settings

logger = get_logger(name=__name__)


class MongoRepository:
    """Минимальный репозиторий для работы с MongoDB (только для absorber)"""

    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = self._get_client()
        self.database: Optional[AsyncIOMotorDatabase] = self._connect_to_db()

    def _get_client(self) -> Optional[AsyncIOMotorClient]:
        """Подключение клиента"""
        try:
            client = AsyncIOMotorClient(
                settings.get_mongo_connection_link,
                serverSelectionTimeoutMS=5000
            )
            return client
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к клиенту MongoDB: {e}")
            return None

    def _connect_to_db(self):
        """Подключение к бд"""
        try:
            database = self.client[settings.MONGO_DB_NAME]
            logger.info(f"✅ Подключение к MongoDB установлено: {settings.MONGO_DB_NAME}")
            return database

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД MongoDB: {e}")
            return None

    async def is_connected(self) -> bool:
        """Подключение к MongoDB"""
        try:
            # Проверка подключения
            is_connected = await self.client.admin.command('ping')
            return is_connected

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к MongoDB: {e}")
            return False

    async def disconnect(self):
        """Закрытие подключения"""
        if self.client:
            try:
                self.client.close()
                logger.info("🔌 Подключение к MongoDB закрыто")
            except Exception as e:
                logger.error(f"❌ Ошибка при закрытии подключения: {e}")

    async def is_connected(self) -> bool:
        """Проверка подключения"""
        if not self.client:
            return False
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка проверки подключения: {e}")
            return False

    def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """Получение коллекции"""
        # ИСПРАВЛЕНО: была ошибка в логике проверки
        if self.database is None:  # <-- БЫЛО: if self.database is not None
            raise ValueError("Database not connected")
        return self.database[collection_name]

    async def find_documents(
            self,
            collection_name: str,
            filter_query: Dict[str, Any] = None,
            limit: int = None,
            sort: List[tuple] = None
    ) -> List[Dict[str, Any]]:
        """Поиск документов (основной метод для absorber)"""
        try:
            # Добавим дополнительную проверку подключения
            if self.database is None:
                logger.error("❌ База данных не подключена")
                return []

            collection = self.get_collection(collection_name)

            # Базовый запрос
            cursor = collection.find(filter_query or {})

            # Сортировка (важно для absorber - сортировка по _id)
            if sort:
                cursor = cursor.sort(sort)

            # Лимит (для batch processing)
            if limit:
                cursor = cursor.limit(limit)

            documents = await cursor.to_list(length=limit)

            # Конвертируем ObjectId в строки и переименовываем _id в mongo_id
            for doc in documents:
                if "_id" in doc:
                    doc["mongo_id"] = str(doc["_id"])  # Сохраняем как mongo_id
                    del doc["_id"]  # Удаляем оригинальное поле _id

            return documents

        except Exception as e:
            logger.error(f"❌ Ошибка поиска в {collection_name}: {e}")
            return []
