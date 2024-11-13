from uuid import UUID

from pydantic import BaseModel


class RawFile(BaseModel):
    name: str
    path: str
    content_type: str
    given_name: str
    pdf_source_url: str
    trace_id: UUID
