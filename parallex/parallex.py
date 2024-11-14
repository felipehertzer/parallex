import asyncio
import tempfile
from typing import Callable, Optional

from parallex.ai.batch_processor import wait_for_batch_completion, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_output
from parallex.ai.uploader import upload_image_for_processing
from parallex.file_management.converter import convert_pdf_to_images
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.image_file import ImageFile
from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.models.upload_batch import UploadBatch
from parallex.utils.logger import logger, setup_logger


# TODO pdf_source_url: str change to be URL or path
async def parallex(
    model: str,
    pdf_source_url: str,
    post_process_callable: Optional[Callable[..., None]] = None,
    concurrency: int = 20,
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

        upload_semaphore = asyncio.Semaphore(concurrency)
        batch_tasks = []
        for image_file in image_files:
            batch_task = asyncio.create_task(
                _create_image_and_batch_job(
                    image_file=image_file,
                    client=open_ai_client,
                    temp_directory=temp_directory,
                    semaphore=upload_semaphore,
                )
            )
            batch_tasks.append(batch_task)
        batches = await asyncio.gather(*batch_tasks)

        logger.debug(f"batches done. total batches- {len(batches)} - {trace_id}")

        page_tasks = []
        process_semaphore = asyncio.Semaphore(concurrency)
        for batch in batches:
            page_task = asyncio.create_task(
                _wait_and_create_pages(
                    batch=batch, client=open_ai_client, semaphore=process_semaphore
                )
            )
            page_tasks.append(page_task)
        pages = await asyncio.gather(*page_tasks)

        logger.debug(f"pages done. total pages- {len(pages)} - {trace_id}")
        sorted_page_responses = sorted(pages, key=lambda x: x.page_number)

        # TODO add combined version of MD to output
        callable_output = ParallexCallableOutput(
            file_name=raw_file.given_name,
            pdf_source_url=raw_file.pdf_source_url,
            trace_id=trace_id,
            pages=sorted_page_responses,
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
        page_response = await process_output(
            client=client, output_file_id=output_file_id, page_number=batch.page_number
        )
        await _remove_global_batch_files(client=client, batch=batch)
        logger.debug(f"page_response: {page_response.page_number}")
        return page_response


async def _remove_global_batch_files(client: OpenAIClient, batch: UploadBatch):
    file_ids = [batch.input_file_id, batch.output_file_id, batch.error_file_id]
    for file_id in file_ids:
        await client.delete_file(file_id)


async def _create_image_and_batch_job(
    image_file: ImageFile,
    client: OpenAIClient,
    temp_directory: str,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
        logger.debug(
            f"uploading image - {image_file.page_number} - {image_file.trace_id}"
        )
        batch_file = await upload_image_for_processing(
            client=client, image_file=image_file, temp_directory=temp_directory
        )
        logger.debug(
            f"finished uploading image - {image_file.page_number} - {image_file.trace_id}"
        )
        logger.debug(
            f"creating batch for image - {image_file.page_number} - {image_file.trace_id}"
        )
        batch = await create_batch(
            client=client,
            file_id=batch_file.id,
            trace_id=image_file.trace_id,
            page_number=image_file.page_number,
        )
        logger.debug(
            f"finished batch for image - {image_file.page_number} - {image_file.trace_id}"
        )

        return batch
