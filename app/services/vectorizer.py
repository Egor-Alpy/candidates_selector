import aiohttp
import logging

from app.core.logger import get_logger

logger = get_logger(name=__name__)


class Vectorizer:
    def __init__(self, api_url: str = "http://matcher-semantic.angora-ide.ts.net:8000"):
        self.api_url = api_url
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def compare_two_strings(self, strings1: str, strings2: str) -> float:
        try:
            lower_string_1 = strings1.lower()
            lower_string_2 = strings2.lower()

            payload = [lower_string_1, lower_string_2]

            session = await self._get_session()
            url = f"{self.api_url}/api/v1/comparsion/strings"

            async with session.post(url, json=payload) as response:
                # logger.info(await response.json())
                if response.status == 200:
                    result = await response.json()
                    score = result.get("similarity", result.get("score", 0.0))
                    return score
                    # return max(0.0, min(1.0, float(score)))
                else:
                    return 0.0

        except Exception as e:
            logging.error(f"Ошибка при вычислении схожести: {e}")
            return 0.0

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
