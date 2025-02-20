import asyncio
from uuid import UUID
from typing import Optional

from openai import BadRequestError, APIError

from parallex.ai.open_ai_client import OpenAIClient
from parallex.exceptions.BatchCreationError import BatchCreationError
from parallex.exceptions.BatchProcessingError import BatchProcessingError
from parallex.models.upload_batch import build_batch, UploadBatch
from parallex.utils.logger import logger


async def create_batch(
    client: OpenAIClient, file_id: str, trace_id: UUID
) -> UploadBatch | None:
    """Creates a Batch for the given file_id"""
    max_retries = 10
    backoff_delay = 5

    for attempt in range(max_retries):
        try:
            batch_response = await client.create_batch(upload_file_id=file_id)
            batch = build_batch(open_ai_batch=batch_response, trace_id=trace_id)
            return batch
        except BadRequestError as e:
            logger.warning(f"BadRequestError on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:
                raise BatchCreationError(
                    f"Failed to create batch after {max_retries} attempts: {str(e)}"
                )
            await asyncio.sleep(backoff_delay)
            backoff_delay *= 2
        except APIError as e:
            logger.error(f"APIError on attempt {attempt + 1}: {str(e)}")
            raise BatchCreationError(
                f"API error occurred while creating batch: {str(e)}"
            )


async def wait_for_batch_completion(
    client: OpenAIClient, batch: UploadBatch
) -> Optional[str]:
    """Waits for Batch to complete and returns output_file_id when available"""
    status = "validating"
    delay = 5
    max_attempts = 60  # 30 minutes maximum wait time
    attempts = 0

    while status not in ("completed", "failed", "canceled"):
        await asyncio.sleep(delay)
        try:
            batch_response = await client.retrieve_batch(batch.id)
            status = batch_response.status
            delay = 30
            attempts += 1

            if status == "completed":
                return batch_response.output_file_id
            elif status == "failed":
                error_message = getattr(batch_response, "error", "Unknown error")
                raise BatchProcessingError(f"Batch processing failed: {error_message}")
            elif status == "canceled":
                raise BatchProcessingError("Batch processing was canceled")

            if attempts >= max_attempts:
                raise BatchProcessingError("Batch processing timed out")

        except APIError as e:
            logger.error(f"APIError while retrieving batch status: {str(e)}")
            raise BatchProcessingError(
                f"API error occurred while retrieving batch status: {str(e)}"
            )

    return None
