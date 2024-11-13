from uuid import UUID

from pydantic import BaseModel, Field

from parallex.models.page_response import PageResponse


class ParallexCallableOutput(BaseModel):
    file_name: str = Field(description="Name of file that is processed")
    pdf_source_url: str
    trace_id: UUID
    pages: list[PageResponse]
