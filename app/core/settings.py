from urllib.parse import quote_plus

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Общие настройки
    PROJECT_NAME: str = "Candidates Selector"
    PROJECT_DESCRIPTION: str = "Сервис для отбора кандидатов мэтчинга"
    PROJECT_VERSION: str = "1.0.0"

    # Настройка логирования
    LOG_LEVEL: str = (
        "INFO"  # Доступные уровни логирования - DEBUG, INFO, WARNING, ERROR, FATAL
    )
    LOG_FORMAT: str = (
        "%(asctime).19s | %(levelname).3s | %(message)s"  # Формат отображения логов
    )

    # Настройка api (fastapi)
    API_HOST: str = "localhost"
    API_PORT: int = 8012

    # Настройка подключения к ElasticSearch
    ES_HOST: str = "localhost"
    ES_PORT: int = 9200
    ES_INDEX: str = "products_v7"
    ES_MAX_RETRIES: int = 3

    # Внешние сервисы
    SERVICE_LINK_ATTRS_STANDARDIZER: str = "http://localhost:8000"
    SERVICE_LINK_UNIT_STANDARDIZER: str = "http://localhost:8001"
    SERVICE_LINK_SEMANTIC_MATCHER: str = "http://localhost:8006"

    # Настройка подключения к MongoDB
    MONGO_DB_HOST: str = "localhost"
    MONGO_DB_PORT: int = 40001
    MONGO_DB_USER: str = "parser"
    MONGO_DB_PASS: str = "password"
    MONGO_DB_NAME: str = "categorized_products"
    MONGO_COLLECTION_NAME: str = "categorized_products"
    MONGO_AUTHMECHANISM: str = "SCRAM-SHA-256"
    MONGO_AUTHSOURCE: str = "admin"
    MONGO_REPLICA_SET: str = "parser-mongodb"
    MONGO_TLS: bool = True
    MONGO_TLS_CA_FILE: str = "/Users/alpy/Downloads/_mongodb-ca-gitignore"
    MONGO_DIRECT_CONNECTION: bool = False

    # Настройки RabbitMQ для FastStream
    RABBITMQ_HOST: str = 'localhost'
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = 'admin'
    RABBITMQ_PASS: str = '5brXrRUhQy8Sl8gs'
    RABBITMQ_VHOST: str = '/'

    # PostgreSQL Configuration
    PG_HOST: str = 'localhost'
    PG_USER: str = 'app'
    PG_PASS: str = 'postgres'
    PG_PORT: int = 5432
    PG_DB_NAME: str = 'app'

    # PostgreSQL Pool Configuration
    PG_POOL_SIZE: int = 5
    PG_MAX_OVERFLOW: int = 10
    PG_POOL_RECYCLE: int = 300
    PG_POOL_PRE_PING: bool = True
    PG_ECHO: bool = False

    # Database Session Configuration
    DB_EXPIRE_ON_COMMIT: bool = False
    DB_AUTOFLUSH: bool = False
    DB_AUTOCOMMIT: bool = False

    # Получение ссылки для подключения к MongoDB
    @property
    def get_mongo_connection_link(self):
        if self.MONGO_DB_USER and self.MONGO_DB_PASS:
            # Базовая строка подключения
            connection_string = (
                f"mongodb://{self.MONGO_DB_USER}:{quote_plus(self.MONGO_DB_PASS)}@"
                f"{self.MONGO_DB_HOST}:{self.MONGO_DB_PORT}/"
            )

            # Добавляем параметры
            params = [f"authSource={self.MONGO_AUTHSOURCE}", f"authMechanism={self.MONGO_AUTHMECHANISM}"]

            if self.MONGO_REPLICA_SET:
                params.append(f"replicaSet={self.MONGO_REPLICA_SET}")

            if self.MONGO_TLS:
                params.append("tls=true")
                if self.MONGO_TLS_CA_FILE:
                    params.append(f"tlsCAFile={self.MONGO_TLS_CA_FILE}")

            if not self.MONGO_DIRECT_CONNECTION and self.MONGO_REPLICA_SET:
                # directConnection должно быть false при использовании replica set
                params.append("directConnection=false")

            # Соединяем все параметры
            connection_string += "?" + "&".join(params)
        else:
            connection_string = f"mongodb://{self.MONGO_DB_HOST}:{self.MONGO_DB_PORT}"

        return connection_string

    # Получение ссылки для подключения к ElasticSearch
    @property
    def get_elastic_dsn(self) -> str:
        return f"http://{self.ES_HOST}:{self.ES_PORT}"

    # Получение ссылки для подключения к RabbitMQ
    @property
    def get_rabbitmq_dsn(self) -> str:
        return f'amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASS}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}{self.RABBITMQ_VHOST}'

    # Получение ссылки для подключения к PostgreSQL
    @property
    def get_postgres_dsn(self) -> str:
        return (
            f'postgresql+asyncpg://{self.PG_USER}:{self.PG_PASS}@'
            f'{self.PG_HOST}:{self.PG_PORT}/{self.PG_DB_NAME}'
        )
    class Config:
        env_file = "../.env"


settings = Settings()
