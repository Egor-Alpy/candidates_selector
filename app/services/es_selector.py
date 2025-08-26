from app.core.es_settings import ElasticQueries
from app.core.logger import get_logger
from app.core.settings import settings
from app.repository.elastic import ElasticRepository

logger = get_logger(name=__name__)


class ElasticSearchSelector:
    def __init__(self, es_repo: ElasticRepository = None):
        self.es_repo = es_repo

    async def find_candidates(self, index_name: str, body: dict):
        """Поиск кандидатов"""
        try:
            candidates = await self.es_repo.make_query(index_name=index_name, body=body)
            return candidates

        except Exception as e:
            logger.error(f'Ошибка при поиске кандидатов в селекторе: {e}')
            return []
