from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from parallex.models.page_response import PageResponse


class ParallexCallableOutput(BaseModel):
    file_name: str = Field(description="Name of file that is processed")
    pdf_source_url: Optional[str] = Field(
        description="Given URL of the source of output"
    )
    trace_id: UUID = Field(description="Unique trace for each file")
    pages: list[PageResponse] = Field(description="List of PageResponse objects")
