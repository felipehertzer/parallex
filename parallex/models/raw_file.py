from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import Field


class RawFile(BaseModel):
    name: str
    path: str
    content_type: str
    trace_id: UUID
