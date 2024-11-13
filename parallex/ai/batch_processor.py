import asyncio
import time
from uuid import UUID

from openai import BadRequestError

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.upload_batch import build_batch, UploadBatch


# TODO do better with backoff
async def create_batch(client: OpenAIClient, file_id: str, trace_id: UUID, page_number: int) -> UploadBatch:
    max_retries = 5
    backoff_delay = 5

    for attempt in range(max_retries):
        try:
            batch_response = client.create_batch(upload_file_id=file_id)
            batch = build_batch(open_ai_batch=batch_response, trace_id=trace_id, page_number=page_number)
            return batch  # Return batch if successful

        except BadRequestError as e:
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(backoff_delay)
            backoff_delay *= 2


async def wait_for_batch_completion(client: OpenAIClient, batch_id) -> str:
    # TODO pass in UploadBatch and mutate?
    # How to process? FIFO?
    # TODO handle "failed", "canceled"
    # There will be a error_file_id for errored jobs
    status = "validating"
    while status not in ("completed", "failed", "canceled"):
        time.sleep(5)
        batch_response = client.retrieve_batch(batch_id)
        status = batch_response.status
        if status == "completed":
            return batch_response.output_file_id
