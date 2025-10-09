from app.core.es_settings import ElasticQueries
from app.core.logger import get_logger
from app.models.tenders import TenderPositions
from app.repository.elastic import ElasticRepository

logger = get_logger(name=__name__)


class ElasticSelector:
    def __init__(self, es_repo: ElasticRepository = None):
        self.es_repo = es_repo

    async def find_candidates(self, index_name: str, body: dict):
        """Поиск кандидатов"""
        try:
            candidates = await self.es_repo.make_query(index_name=index_name, body=body)
            logger.info(len(candidates['hits']['hits']))
            return candidates

        except Exception as e:
            logger.error(f'Ошибка при поиске кандидатов в селекторе: {e}')
            return []

    async def find_candidates_for_rabbit(self, index_name: str, position: TenderPositions):
        """Поиск кандидатов"""
        try:
            body = ElasticQueries.get_query_v6(
                position=position
            )
            # logger.info(f'body: {body}')
            candidates = await self.es_repo.make_query(index_name=index_name, body=body)
            # logger.info(f"candidates: {candidates}")
            return candidates

        except Exception as e:
            logger.error(f'Ошибка при поиске кандидатов в селекторе: {e}')
            return []
