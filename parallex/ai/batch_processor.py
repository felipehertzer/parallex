import asyncio
from uuid import UUID

from openai import BadRequestError

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.upload_batch import build_batch, UploadBatch


async def create_batch(
    client: OpenAIClient, file_id: str, trace_id: UUID, page_number: int
) -> UploadBatch:
    """Creates a Batch for the given file_id"""
    max_retries = 10
    backoff_delay = 5

    for attempt in range(max_retries):
        try:
            batch_response = await client.create_batch(upload_file_id=file_id)
            batch = build_batch(
                open_ai_batch=batch_response, trace_id=trace_id, page_number=page_number
            )
            return batch  # Return batch if successful
        except BadRequestError as e:
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(backoff_delay)
            backoff_delay *= 2


# TODO handle errors
async def wait_for_batch_completion(client: OpenAIClient, batch: UploadBatch) -> str:
    """Waits for Batch to complete and returns output_file_id when available"""
    status = "validating"
    delay = 5
    while status not in ("completed", "failed", "canceled"):
        await asyncio.sleep(delay)
        batch_response = await client.retrieve_batch(batch.id)
        status = batch_response.status
        if status == "completed":
            batch.output_file_id = batch_response.output_file_id
            return batch_response.output_file_id
        else:
            delay = 30
