import asyncio
import logging

from app.core.logger import get_logger
from app.core.settings import settings
from app.core.connection_pool import connection_pool

logger = get_logger(name=__name__)


class SemanticMatcher:
    def __init__(self, api_url: str = settings.SERVICE_LINK_SEMANTIC_MATCHER):
        self.api_url = api_url

    async def compare_two_strings(self, string1: str, string2: str) -> float:
        for attempt in range(3):
            try:
                session = await connection_pool.get_http_session('semantic_matcher')
                url = f"{self.api_url}/api/v1/comparsion/strings"
                payload = [string1, string2]

                async with session.post(url, json=payload, ssl=False) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("score", 0.0)
                    else:
                        logger.info(response.status)
                        return 0.0

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                else:
                    logging.error(f"Ошибка при семантическом сравнении {string1} - {string2}: {e}")
                    return 0.0
        return 0.0

    async def compare_strings_batch(self, names_similarity_list: list[list[str]]) -> list[float]:
        """Отправка запроса на семантическое сравнение строк батчем"""
        for attempt in range(3):
            try:
                session = await connection_pool.get_http_session('semantic_matcher')
                url = f"{self.api_url}/api/v1/comparsion/strings/batch"
                payload = names_similarity_list

                async with session.post(url, json=payload, ssl=False) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        logger.info(response.status)
                        return []

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                else:
                    logging.error(f"Ошибка при батчевом семантическом сравнении {names_similarity_list}: {e}")
                    return []
        return []