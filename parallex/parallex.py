import asyncio
import tempfile
from typing import Callable

from parallex.ai.batch_processor import wait_for_batch_completion, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_output
from parallex.ai.uploader import upload_image_for_processing
from parallex.file_management.converter import convert_pdf_to_images
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.models.parallex_ouput import ParallexOutput
from parallex.utils.logger import logger


# TODO pdf_source_url: str change to be URL or path
# TODO post_process_callable
# TODO concurrency as arg
async def parallex(
    model: str,
    pdf_source_url: str,
    post_process_callable: Callable[..., None],
    concurrency: int = 10,
) -> ParallexOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
        open_ai_client = OpenAIClient(model=model)

        # Fetch PDF and add to temp dir
        raw_file = await add_file_to_temp_directory(
            pdf_source_url=pdf_source_url, temp_directory=temp_directory
        )
        trace_id = raw_file.trace_id
        logger.info(f"inside parallex -- file_name: {raw_file.name}")

        # Convert PDF to image
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
                    trace_id=trace_id,
                    semaphore=upload_semaphore,
                )
            )
            batch_tasks.append(batch_task)
        batches = await asyncio.gather(*batch_tasks)

        logger.info(f"batches done. total batches- {len(batches)} - {trace_id}")

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

        logger.info(f"pages done. total pages- {len(pages)} - {trace_id}")
        sorted_page_responses = sorted(pages, key=lambda x: x.page_number)

        # TODO add combined version of MD to output
        callable_output = ParallexCallableOutput(
            file_name=raw_file.given_name,
            pdf_source_url=raw_file.pdf_source_url,
            trace_id=trace_id,
            pages=sorted_page_responses,
        )
        post_process_callable(output=callable_output)
    return ParallexOutput(name="TODO")


async def _wait_and_create_pages(batch, client, semaphore):
    async with semaphore:
        logger.info(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        output_file_id = await wait_for_batch_completion(
            client=client, batch_id=batch.id
        )
        logger.info(f"batch completed - {batch.id} - {batch.trace_id}")
        page_response = await process_output(
            client=client, output_file_id=output_file_id, page_number=batch.page_number
        )
        logger.info(f"page_response: {page_response.page_number}")
        return page_response


async def _create_image_and_batch_job(
    image_file, client, temp_directory, trace_id, semaphore
):
    async with semaphore:
        logger.info(f"uploading image - {image_file.page_number} - {trace_id}")
        batch_file = await upload_image_for_processing(
            client=client, image_file=image_file, temp_directory=temp_directory
        )
        logger.info(f"finished uploading image - {image_file.page_number} - {trace_id}")
        logger.info(f"creating batch for image - {image_file.page_number} - {trace_id}")
        batch = await create_batch(
            client=client,
            file_id=batch_file.id,
            trace_id=trace_id,
            page_number=image_file.page_number,
        )
        logger.info(f"finished batch for image - {image_file.page_number} - {trace_id}")

        return batch
