# Candidates Selector Service

Сервис для автоматического отбора и мэтчинга кандидатов товаров по позициям тендеров.

## Функциональность

- **Автоматическая обработка тендеров** через RabbitMQ
- **Поиск кандидатов** в Elasticsearch по названию и категории
- **Сравнение атрибутов** позиций с товарами из базы
- **Семантический анализ** названий и характеристик
- **API для ручного тестирования** отбора кандидатов

## Быстрый запуск

### Требования

- Docker и Docker Compose
- RabbitMQ
- Elasticsearch  
- PostgreSQL

### Запуск

1. Настройка в `.env` файле:
```env
# Fastapi
API_HOST=localhost
API_PORT=8000

# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
RABBITMQ_VHOST=/
RABBITMQ_PORT=5672

# Elasticsearch
ES_HOST=localhost
ES_PORT=9200
ES_INDEX=products_v1

# PostgreSQL
PG_HOST=localhost
PG_USER=app
PG_PASS=postgres
PG_PORT=5432
PG_DB_NAME=app

# Внешние сервисы
SERVICE_LINK_ATTRS_STANDARDIZER=http://localhost:8000
SERVICE_LINK_UNIT_STANDARDIZER=http://localhost:8001
SERVICE_LINK_SEMANTIC_MATCHER=http://localhost:8081
```


## Использование

### Worker режим
Сервис автоматически обрабатывает сообщения из RabbitMQ очереди `matching_queue` с routing key `tender.categorized`.

## Архитектура

```
RabbitMQ → Worker → Elasticsearch → PostgreSQL
                 ↓
             Shrinker (анализ атрибутов)
                 ↓
         Внешние сервисы (стандартизация)
```

## Компоненты

- **FastAPI** - REST API
- **FastStream** - RabbitMQ обработчик
- **Elasticsearch** - поиск товаров
- **PostgreSQL** - тендеры и результаты
- **Shrinker** - анализ и сравнение атрибутов

## Логи

Для изменения уровня логирования нужно поменять переменную LOG_LEVEL в settings.py
