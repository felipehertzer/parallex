from uuid import UUID

from pydantic import BaseModel, Field


class BatchFile(BaseModel):
    id: str = Field(description="ID of the OpenAI Batch")
    name: str = Field(description="Name of file batch was created with")
    purpose: str = Field(description="Purpose 'batch")
    status: str = Field(description="Status of the batch")
    trace_id: UUID = Field(description="Unique trace for each file")
