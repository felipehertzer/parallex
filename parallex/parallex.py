import asyncio
import tempfile
from typing import Callable, Optional
from uuid import UUID

from parallex.ai.batch_processor import wait_for_batch_completion, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_output
from parallex.ai.uploader import upload_images_for_processing
from parallex.file_management.converter import convert_pdf_to_images
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.image_file import ImageFile
from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.models.upload_batch import UploadBatch
from parallex.utils.constants import DEFAULT_PROMPT
from parallex.utils.logger import logger, setup_logger


# TODO pdf_source_url: str change to be URL or path
async def parallex(
    model: str,
    pdf_source_url: str,
    post_process_callable: Optional[Callable[..., None]] = None,
    concurrency: int = 20,
    prompt_text: str = DEFAULT_PROMPT,
    log_level: str = "ERROR",
) -> ParallexCallableOutput:
    setup_logger(log_level)
    with tempfile.TemporaryDirectory() as temp_directory:
        open_ai_client = OpenAIClient(model=model)

        raw_file = await add_file_to_temp_directory(
            pdf_source_url=pdf_source_url, temp_directory=temp_directory
        )
        trace_id = raw_file.trace_id
        image_files = await convert_pdf_to_images(
            raw_file=raw_file, temp_directory=temp_directory
        )

        batch_files = await upload_images_for_processing(
            client=open_ai_client,
            image_files=image_files,
            temp_directory=temp_directory,
            prompt_text=prompt_text,
        )
        start_batch_semaphore = asyncio.Semaphore(concurrency)
        start_batch_tasks = []
        for file in batch_files:
            batch_task = asyncio.create_task(
                _create_images_and_batch_jobs(
                    batch_file=file,
                    client=open_ai_client,
                    trace_id=trace_id,
                    semaphore=start_batch_semaphore,
                )
            )
            start_batch_tasks.append(batch_task)
        batches = await asyncio.gather(*start_batch_tasks)

        pages_tasks = []
        process_semaphore = asyncio.Semaphore(concurrency)
        for batch in batches:
            page_task = asyncio.create_task(
                _wait_and_create_pages(
                    batch=batch, client=open_ai_client, semaphore=process_semaphore
                )
            )
            pages_tasks.append(page_task)
        page_groups = await asyncio.gather(*pages_tasks)

        pages = [page for batch_pages in page_groups for page in batch_pages]
        logger.debug(f"pages done. total pages- {len(pages)} - {trace_id}")
        sorted_pages = sorted(pages, key=lambda x: x.page_number)

        # TODO add combined version of MD to output / save to file system
        callable_output = ParallexCallableOutput(
            file_name=raw_file.given_name,
            pdf_source_url=raw_file.pdf_source_url,
            trace_id=trace_id,
            pages=sorted_pages,
        )
        if post_process_callable is not None:
            post_process_callable(output=callable_output)
        return callable_output


async def _wait_and_create_pages(
    batch: UploadBatch, client: OpenAIClient, semaphore: asyncio.Semaphore
):
    async with semaphore:
        logger.debug(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        output_file_id = await wait_for_batch_completion(client=client, batch=batch)
        logger.debug(f"batch completed - {batch.id} - {batch.trace_id}")
        page_responses = await process_output(
            client=client, output_file_id=output_file_id
        )
        await _remove_global_batch_files(client=client, batch=batch)
        return page_responses


async def _remove_global_batch_files(client: OpenAIClient, batch: UploadBatch):
    file_ids = [batch.input_file_id, batch.output_file_id, batch.error_file_id]
    for file_id in file_ids:
        await client.delete_file(file_id)


async def _create_images_and_batch_jobs(
    batch_file: ImageFile,
    client: OpenAIClient,
    trace_id: UUID,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
        batch = await create_batch(
            client=client, file_id=batch_file.id, trace_id=trace_id
        )
        return batch
