import logging

import aiohttp

from app.core.logger import get_logger

logger = get_logger(name=__name__)


class UnitStandardizer:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def normalize_unit(self, value: str, unit: str) -> dict:
        try:
            session = await self._get_session()
            url = f"{self.api_url}/api/v1/normalize"

            payload = {"value": value, "unit": unit}

            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(result)
                    return result
                else:
                    return {}

        except Exception as e:
            logging.error(f"Ошибка при вычленении сущностей из названия и значения атрибутов: {e}")
            return {}

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
