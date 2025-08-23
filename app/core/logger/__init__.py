from .core import ContextLogger
from app.core.settings import settings

def get_logger(level: int = settings.LOG_LEVEL, name=settings.PROJECT_NAME) -> ContextLogger:
    logger = ContextLogger(
        format=settings.LOG_FORMAT,
        project_name=name,
        level=level
    )
    return logger
