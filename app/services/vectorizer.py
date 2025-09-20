import asyncio
import logging

import aiohttp

from app.core.logger import get_logger
from app.core.settings import settings

logger = get_logger(name=__name__)


class SemanticMatcher:
    def __init__(self, api_url: str = settings.SERVICE_LINK_SEMANTIC_MATCHER):
        self.api_url = api_url
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def compare_two_strings(self, string1: str, string2: str) -> float:
        for attempt in range(3):  # 3 попытки
            try:
                async with aiohttp.ClientSession() as session:
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
                if attempt < 2:  # Если не последняя попытка
                    await asyncio.sleep(1)  # Ждем 1 секунду
                    continue
                else:
                    logging.error(
                        f"Ошибка при семантическом сравнении {string1} - {string2}: {e}"
                    )
                    return 0.0
        return 0.0

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
