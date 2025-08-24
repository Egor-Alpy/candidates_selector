from pydantic import BaseModel, Field


class ESCandidatesRequest(BaseModel):
    index_name: str = Field(..., description="Название индекса, в котором будет проводиться поиск")
    position_title: str = Field(..., description="Название тендерной позиции")
    position_yandex_category: str = Field(
        ...,
        description="Подобранное через 'сервис-категоризатор' для тендерной позиции ")
    size: int = Field(...,description="Максимум вывести в консоль кандидатов", ge=1)


class VectorCandidatesRequest(BaseModel):
    position_title: str = Field(..., description="Название тендерной позиции")
