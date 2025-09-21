import logging

from app.core.logger import get_logger
from app.core.settings import settings
from app.core.connection_pool import connection_pool

logger = get_logger(name=__name__)


class AttrsStandardizer:
    def __init__(self, api_url=settings.SERVICE_LINK_ATTRS_STANDARDIZER):
        self.api_url = api_url

    async def extract_attr_data(self, string_to_handle: str):
        try:
            session = await connection_pool.get_http_session('attrs_standardizer')
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