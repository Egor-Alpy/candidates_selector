from pydantic import BaseModel, Field


# Pydantic модели для API
class HealthResponse(BaseModel):
    """Ответ проверки здоровья системы"""
    status: str = Field(..., description="Общий статус системы")
    mongodb: bool = Field(..., description="Статус подключения к MongoDB")
    elasticsearch: bool = Field(..., description="Статус подключения к Elasticsearch")
    running: bool = Field(..., description="Запущен ли мониторинг")
    timestamp: str = Field(..., description="Время проверки")


