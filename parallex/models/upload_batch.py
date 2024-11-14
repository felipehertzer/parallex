from typing import Optional
from uuid import UUID

from openai.types.batch import Errors, Batch
from pydantic import BaseModel, Field


class UploadBatch(BaseModel):
    # page_number: int = Field(description="Page number of associated file")
    trace_id: UUID = Field(description="Unique trace for each file")
    id: str = Field(description="ID of the OpenAI Batch")
    completion_window: str = Field(description="When batch can complete (24hrs)")
    created_at: int = Field(description="When batch was created")
    endpoint: str = Field(description="Endpoint used for retreival")
    input_file_id: str = Field(description="File that is input to batch")
    output_file_id: Optional[str] = Field(
        None, description="File that is output when batch completes"
    )
    status: str = Field(description="Current status of the batch")
    cancelled_at: Optional[int] = Field(None, description="When batch cancelled")
    cancelling_at: Optional[int] = Field(
        None, description="When batch started cancelling"
    )
    completed_at: Optional[int] = Field(None, description="When batch completed")
    expired_at: Optional[int] = Field(None, description="When batch expired")
    expires_at: Optional[int] = Field(None, description="When batch expires")
    failed_at: Optional[int] = Field(None, description="When batch failed")
    finalizing_at: Optional[int] = Field(
        None, description="When batch started finalizing"
    )
    in_progress_at: Optional[int] = Field(
        None, description="When batch started processing"
    )
    error_file_id: Optional[str] = Field(
        None, description="File that is created during error of batch"
    )
    errors: Optional[Errors] = Field(None, description="List of errors")


def build_batch(open_ai_batch: Batch, trace_id: UUID) -> UploadBatch:
    fields = UploadBatch.model_fields
    input_fields = {key: getattr(open_ai_batch, key, None) for key in fields}
    input_fields["trace_id"] = trace_id
    # input_fields["page_number"] = page_number
    return UploadBatch(**input_fields)
