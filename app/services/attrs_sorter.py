import aiohttp
import logging

from app.core.logger import get_logger

logger = get_logger(name=__name__)


class AttrsSorter:
    def __init__(self, api_url: str = "http://localhost:8000"):
        self.api_url = api_url
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def extract_attr_data(self, string_to_handle: str):
        try:
            session = await self._get_session()
            url = f"{self.api_url}/standardize"

            payload = [string_to_handle]

            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(result)
                    return result
                else:
                    return None

        except Exception as e:
            logging.error(f"Ошибка при вычленении сущностей из названия и значения характеристики: {e}")
            return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


attr_sorter = AttrsSorter()

import asyncio

asyncio.run(attr_sorter.extract_attr_data('чикен макнагец 20 мп: 3 рубля'))


