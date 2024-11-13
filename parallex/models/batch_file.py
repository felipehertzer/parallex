from uuid import UUID

from pydantic import BaseModel


class BatchFile(BaseModel):
    id: str
    name: str
    purpose: str
    status: str
    trace_id: UUID
