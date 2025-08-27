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
    API_PORT: int = 8011

    # Настройка подключения к ElasticSearch
    ES_HOST: str = "elasticsearch"
    ES_PORT: int = 9200
    ES_INDEX: str = "products_v1"
    ES_MAX_RETRIES: int = 3

    # Настройка подключения к MongoDB
    # MONGO_DB_HOST: str = "alpy"
    # MONGO_DB_PORT: int = 27017
    # MONGO_DB_USER: str = "alpy"
    # MONGO_DB_PASS: str = "NDB5A+alpy"
    # MONGO_DB_NAME: str = "alpy"
    # MONGO_AUTHMECHANISM: str = "alpy"
    # MONGO_AUTHSOURCE: str = "alpy"
    # MONGO_DIRECT_CONNECTION: str = 'alpy'

    # Настройка подключения к MongoDB
    MONGO_DB_HOST: str = "mongodb.angora-ide.ts.net"
    MONGO_DB_PORT: int = 27017
    MONGO_DB_USER: str = "parser"
    MONGO_DB_PASS: str = "NDB5A+Uv7hZ4pNnANQwVpMK5C2VpL30NsDkVDzpaKMtCqPV2"
    MONGO_DB_NAME: str = "categorized_products"
    MONGO_COLLECTION_NAME: str = "categorized_products"
    MONGO_AUTHMECHANISM: str = "SCRAM-SHA-256"
    MONGO_AUTHSOURCE: str = "admin"
    MONGO_DIRECT_CONNECTION: str = "true"

    # Настройки RabbitMQ для FastStream
    RABBITMQ_HOST: str = 'rabbitmq.angora-ide.ts.net'
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = 'admin'
    RABBITMQ_PASS: str = '5brXrRUhQy8Sl8gs'
    RABBITMQ_VHOST: str = '/'

    # Получение ссылки для подключения к MongoDB
    @property
    def get_mongo_connection_link(self):
        if settings.MONGO_DB_USER and settings.MONGO_DB_PASS:
            connection_string = (
                f"mongodb://{settings.MONGO_DB_USER}:{quote_plus(settings.MONGO_DB_PASS)}@"
                f"{settings.MONGO_DB_HOST}:{settings.MONGO_DB_PORT}/{settings.MONGO_AUTHSOURCE}"
                f"?authMechanism={settings.MONGO_AUTHMECHANISM}&directConnection={settings.MONGO_DIRECT_CONNECTION}"
            )
        else:
            connection_string = (
                f"mongodb://{settings.MONGO_DB_HOST}:{settings.MONGO_DB_PORT}"
            )

        return connection_string

    # Получение ссылки для подключения к ElasticSearch
    @property
    def get_elastic_dsn(self) -> str:
        return f"http://{self.ES_HOST}:{self.ES_PORT}"

        # Получение ссылки для подключения к RabbitMQ

    @property
    def get_rabbitmq_dsn(self) -> str:
        return f'amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASS}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}{self.RABBITMQ_VHOST}'

    class Config:
        env_file = ".env"


settings = Settings()
