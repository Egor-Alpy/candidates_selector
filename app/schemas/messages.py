from typing import Optional
from pydantic import BaseModel


class TenderCreatedMessage(BaseModel):
    """Сообщение о созданном тендере"""
    tender_id: int
    tender_number: Optional[str] = None
    customer_name: Optional[str] = None