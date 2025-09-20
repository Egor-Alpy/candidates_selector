import aiohttp
import logging

from app.core.logger import get_logger
from app.core.settings import settings

logger = get_logger(name=__name__)


class AttrsStandardizer:
    def __init__(self, api_url=settings.SERVICE_LINK_ATTRS_STANDARDIZER):
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
            async with aiohttp.ClientSession() as session:
                url = f"{self.api_url}/standardize"

                payload = [string_to_handle]

                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        return None

        except Exception as e:
            logging.error(f"Ошибка при вычленении сущностей из названия и значения характеристики: {e}")
            return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
