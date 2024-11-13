from pydantic import BaseModel, Field

from parallex.models.page_response import PageResponse


class ParallexCallableOutput(BaseModel):
    file_name: str = Field(description="Name of file that is processed")
    pages: list[PageResponse]
