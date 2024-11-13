from typing import Optional
from uuid import UUID

from openai.types.batch import Errors, Batch
from pydantic import BaseModel, Field


class UploadBatch(BaseModel):
    id: str
    completion_window: str
    created_at: int
    endpoint: str
    input_file_id: str
    output_file_id: Optional[str] = Field(None, description="thing")
    status: str
    cancelled_at: Optional[int] = Field(None, description="thing")
    cancelling_at: Optional[int] = Field(None, description="thing")
    completed_at: Optional[int] = Field(None, description="thing")
    expired_at: Optional[int] = Field(None, description="thing")
    expires_at: Optional[int] = Field(None, description="thing")
    failed_at: Optional[int] = Field(None, description="thing")
    finalizing_at: Optional[int] = Field(None, description="thing")
    in_progress_at: Optional[int] = Field(None, description="thing")
    error_file_id: Optional[str] = Field(None, description="thing")
    errors: Optional[Errors] = Field(None, description="thing")
    trace_id: UUID

def build_batch(open_ai_batch: Batch, trace_id: UUID) -> UploadBatch:
    fields = UploadBatch.model_fields
    input_fields = {key: getattr(open_ai_batch, key, None) for key in fields}
    input_fields['trace_id'] = trace_id
    return UploadBatch(**input_fields)
