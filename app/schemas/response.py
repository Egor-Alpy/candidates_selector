from typing import Any

from pydantic import BaseModel, Field


class VectorCandidatesResponse(BaseModel):
    status: bool = Field(..., description="Статус выполнения задачи")
    candidates: list[dict] = Field(..., description="Подобранные кандидаты ")


class ESCandidatesResponse(BaseModel):
    status: bool = Field(..., description="Статус выполнения задачи")
    candidates: Any = Field(..., description="Подобранные кандидаты ")
