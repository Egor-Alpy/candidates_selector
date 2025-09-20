import asyncio
import logging

import aiohttp

from app.core.logger import get_logger
from app.core.settings import settings

logger = get_logger(name=__name__)


class UnitStandardizer:
    def __init__(self, api_url: str = settings.SERVICE_LINK_UNIT_STANDARDIZER):
        self.api_url = api_url
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def normalize_unit(self, value: str, unit: str) -> dict:
        for attempt in range(3):  # 3 попытки
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"{self.api_url}/api/v1/normalize"
                    payload = {"value": value, "unit": unit}

                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result
                        else:
                            return {}

            except Exception as e:
                if attempt < 2:  # Если не последняя попытка
                    await asyncio.sleep(1)  # Ждем 1 секунду
                    continue
                else:
                    logging.error(f"Ошибка при стандартизации юнитов: {e}")
                    return {}
        return {}

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
