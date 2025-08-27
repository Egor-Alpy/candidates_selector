from typing import Optional

from app.core.es_settings import ElasticQueries
from app.core.logger import get_logger
from app.core.settings import settings
from app.repository.elastic import ElasticRepository

logger = get_logger(name=__name__)


class ElasticSearchSelector:
    def __init__(self, es_repo: ElasticRepository = None):
        self.es_repo = es_repo

    async def find_candidates(self, index_name: str, position_title: str, yandex_category: str, size: Optional[int]):
        """Поиск кандидатов"""
        try:
            body = ElasticQueries.get_query_v1(
                position_title=position_title,
                yandex_category=yandex_category,
                size=size
            )
            logger.info(f'body: {body}')
            candidates = await self.es_repo.make_query(index_name=index_name, body=body)
            logger.info(f"candidates: {candidates}")
            return candidates

        except Exception as e:
            logger.error(f'Ошибка при поиске кандидатов в селекторе: {e}')
            return []

    async def find_candidates_for_rabbit(self, index_name: str, position_title: str, yandex_category: str):
        """Поиск кандидатов"""
        try:
            body = ElasticQueries.get_query_for_rabbit(
                position_title=position_title,
                yandex_category=yandex_category,
            )
            logger.info(f'body: {body}')
            candidates = await self.es_repo.make_query(index_name=index_name, body=body)
            logger.info(f"candidates: {candidates}")
            return candidates

        except Exception as e:
            logger.error(f'Ошибка при поиске кандидатов в селекторе: {e}')
            return []