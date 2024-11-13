from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import Field


class ImageFile(BaseModel):
    path: str
    page_number: int
    given_file_name: str
    trace_id: UUID
