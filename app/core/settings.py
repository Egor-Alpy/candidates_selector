from urllib.parse import quote_plus

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Общие настройки
    PROJECT_NAME: str = "Candidates Selector"
    PROJECT_DESCRIPTION: str = "Сервис для отбора кандидатов мэтчинга"
    PROJECT_VERSION: str = "1.0.0"

    # Настройка мэтчера
    CANDIDATES_TRASHOLD_SCORE: float = 0.7  # процент смэтченных характеристик по достижению которого кандидат будет сохранен в бд

    THRESHOLD_ATTRIBUTE_MATCH: float = 0.73  # Порог мэтчинга НАЗВАНИЙ характеристик
    THRESHOLD_VALUE_MATCH: float = 0.85  # Порог мэтчинга ЗНАЧЕНИЙ характеристик

    # Настройка логирования
    LOG_LEVEL: str = "INFO"  # Доступные уровни логирования - DEBUG, INFO, WARNING, ERROR, FATAL
    LOG_FORMAT: str = "%(asctime).19s | %(levelname).3s | %(message)s"

    # Настройка api (fastapi)
    API_HOST: str = "localhost"
    API_PORT: int = 8012

    # Настройка подключения к ElasticSearch
    ES_HOST: str = "localhost"
    ES_PORT: int = 9200
    ES_INDEX: str = "products_testik_v3"
    ES_CANDIDATES_QTY: int = 2000
    ES_MAX_RETRIES: int = 3

    # Внешние сервисы
    SERVICE_LINK_ATTRS_STANDARDIZER: str = "http://localhost:8000"
    SERVICE_LINK_UNIT_STANDARDIZER: str = "http://localhost:8001"
    SERVICE_LINK_SEMANTIC_MATCHER: str = "http://localhost:8081"

    # Кол-во одновременно обрабатываемых кандидатов
    SHRINKER_SEMAPHORE_SIZE: int = 100

    # Настройки RabbitMQ для FastStream
    RABBITMQ_HOST: str = 'localhost'
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = 'guest'
    RABBITMQ_PASS: str = 'guest'
    RABBITMQ_VHOST: str = '/'

    # PostgreSQL Configuration
    PG_HOST: str = 'localhost'
    PG_USER: str = 'app'
    PG_PASS: str = 'postgres'
    PG_PORT: int = 5432
    PG_DB_NAME: str = 'app'

    # PostgreSQL Pool Configuration
    PG_POOL_SIZE: int = 15
    PG_MAX_OVERFLOW: int = 25
    PG_POOL_RECYCLE: int = 300
    PG_POOL_PRE_PING: bool = True
    PG_ECHO: bool = False

    # Database Session Configuration
    DB_EXPIRE_ON_COMMIT: bool = False
    DB_AUTOFLUSH: bool = False
    DB_AUTOCOMMIT: bool = False

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
