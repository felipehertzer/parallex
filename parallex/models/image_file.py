from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import Field


class ImageFile(BaseModel):
    path: str = Field(description="Path to the image in temp directory")
    page_number: int = Field(description="Associated page of the PDF")
    given_file_name: str = Field(description="Name of the given file")
    trace_id: UUID = Field(description="Unique trace for each file")
