from pydantic import BaseModel, Field


class ESCandidatesRequest(BaseModel):
    position_title: str = Field(..., description="Название тендерной позиции")
    positon_yandex_category_id: int = Field(
        ...,
        description="Подобранное через 'сервис-категоризатор' yandex_id категории для тендерной позиции ",
    )


class VectorCandidatesRequest(BaseModel):
    position_title: str = Field(..., description="Название тендерной позиции")
