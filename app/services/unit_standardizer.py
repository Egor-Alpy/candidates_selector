import asyncio
import logging

from app.core.logger import get_logger
from app.core.settings import settings
from app.core.connection_pool import connection_pool

logger = get_logger(name=__name__)


class UnitStandardizer:
    def __init__(self, api_url: str = settings.SERVICE_LINK_UNIT_STANDARDIZER):
        self.api_url = api_url

    async def normalize_unit(self, value: str, unit: str) -> dict:
        for attempt in range(3):
            try:
                session = await connection_pool.get_http_session('unit_standardizer')
                url = f"{self.api_url}/api/v1/normalize"
                payload = {"value": value, "unit": unit}

                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        return {}

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                else:
                    logging.error(f"Ошибка при стандартизации юнитов: {e}")
                    return {}
        return {}