from uuid import UUID

from pydantic import BaseModel, Field


class RawFile(BaseModel):
    name: str = Field(description="Name of the file given by Parallex")
    path: str = Field(description="Path to file in temp directory")
    content_type: str = Field(description="Given file type")
    given_name: str = Field(description="Name of file given")
    pdf_source_url: str = Field(description="Source of file")
    trace_id: UUID = Field(description="Unique trace for each file")
