import asyncio
import tempfile
import uuid
from typing import Callable, Optional
from uuid import UUID

from parallex.ai.batch_processor import wait_for_batch_completion, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_images_output, process_prompts_output
from parallex.ai.uploader import (
    upload_images_for_processing,
    upload_prompts_for_processing,
)
from parallex.file_management.converter import convert_pdf_to_images
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.file_management.remote_file_handler import RemoteFileHandler
from parallex.models.batch_file import BatchFile
from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.models.parallex_prompts_callable_output import (
    ParallexPromptsCallableOutput,
)
from parallex.models.upload_batch import UploadBatch
from parallex.utils.constants import DEFAULT_PROMPT
from parallex.utils.logger import logger, setup_logger


# TODO pdf_source_url: str change to be URL or path
async def parallex(
    model: str,
    pdf_source_url: str,
    post_process_callable: Optional[Callable[..., None]] = None,
    concurrency: Optional[int] = 20,
    prompt_text: Optional[str] = DEFAULT_PROMPT,
    log_level: Optional[str] = "ERROR",
) -> ParallexCallableOutput:
    setup_logger(log_level)
    remote_file_handler = RemoteFileHandler()
    open_ai_client = OpenAIClient(model=model, remote_file_handler=remote_file_handler)
    try:
        return await _execute(
            open_ai_client=open_ai_client,
            pdf_source_url=pdf_source_url,
            post_process_callable=post_process_callable,
            concurrency=concurrency,
            prompt_text=prompt_text,
        )
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
    finally:
        await _delete_associated_files(open_ai_client, remote_file_handler)


async def parallex_simple_prompts(
    model: str,
    prompts: list[str],
    post_process_callable: Optional[Callable[..., None]] = None,
    log_level: Optional[str] = "ERROR",
    concurrency: Optional[int] = 20,
) -> ParallexPromptsCallableOutput:
    setup_logger(log_level)
    remote_file_handler = RemoteFileHandler()
    open_ai_client = OpenAIClient(model=model, remote_file_handler=remote_file_handler)
    try:
        return await _prompts_execute(
            open_ai_client=open_ai_client,
            prompts=prompts,
            post_process_callable=post_process_callable,
            concurrency=concurrency,
        )
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
    finally:
        await _delete_associated_files(open_ai_client, remote_file_handler)


async def _prompts_execute(
    open_ai_client: OpenAIClient,
    prompts: list[str],
    post_process_callable: Optional[Callable[..., None]] = None,
    concurrency: Optional[int] = 20,
):
    with tempfile.TemporaryDirectory() as temp_directory:
        trace_id = uuid.uuid4()
        batch_files = await upload_prompts_for_processing(
            client=open_ai_client,
            prompts=prompts,
            temp_directory=temp_directory,
            trace_id=trace_id,
        )
        start_batch_semaphore = asyncio.Semaphore(concurrency)
        start_batch_tasks = []
        for file in batch_files:
            batch_task = asyncio.create_task(
                _create_batch_jobs(
                    batch_file=file,
                    client=open_ai_client,
                    trace_id=trace_id,
                    semaphore=start_batch_semaphore,
                )
            )
            start_batch_tasks.append(batch_task)
        batch_jobs = await asyncio.gather(*start_batch_tasks)

        process_semaphore = asyncio.Semaphore(concurrency)
        prompt_tasks = []
        for batch in batch_jobs:
            logger.info(
                f"waiting for batch to complete - {batch.id} - {batch.trace_id}"
            )
            prompt_task = asyncio.create_task(
                await _wait_and_create_prompt_responses(batch=batch, client=open_ai_client, semaphore=process_semaphore)
            )
            prompt_tasks.append(prompt_task)
        prompt_response_groups = await asyncio.gather(*prompt_tasks)

        flat_responses = [response for batch in prompt_response_groups for response in batch]

        sorted_responses = sorted(flat_responses, key=lambda x: x.prompt_index)
        callable_output = ParallexPromptsCallableOutput(
            original_prompts=prompts,
            trace_id=trace_id,
            responses=sorted_responses,
        )
        if post_process_callable is not None:
            post_process_callable(output=callable_output)
        return callable_output


async def _execute(
    open_ai_client: OpenAIClient,
    pdf_source_url: str,
    post_process_callable: Optional[Callable[..., None]] = None,
    concurrency: Optional[int] = 20,
    prompt_text: Optional[str] = DEFAULT_PROMPT,
) -> ParallexCallableOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
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
                _create_batch_jobs(
                    batch_file=file,
                    client=open_ai_client,
                    trace_id=trace_id,
                    semaphore=start_batch_semaphore,
                )
            )
            start_batch_tasks.append(batch_task)
        batch_jobs = await asyncio.gather(*start_batch_tasks)

        pages_tasks = []
        process_semaphore = asyncio.Semaphore(concurrency)
        for batch in batch_jobs:
            page_task = asyncio.create_task(
                _wait_and_create_pages(
                    batch=batch, client=open_ai_client, semaphore=process_semaphore
                )
            )
            pages_tasks.append(page_task)
        page_groups = await asyncio.gather(*pages_tasks)

        pages = [page for batch_pages in page_groups for page in batch_pages]
        logger.info(f"pages done. total pages- {len(pages)} - {trace_id}")
        sorted_pages = sorted(pages, key=lambda x: x.prompt_index)

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
        logger.info(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        output_file_id = await wait_for_batch_completion(client=client, batch=batch)
        logger.info(f"batch completed - {batch.id} - {batch.trace_id}")
        page_responses = await process_images_output(
            client=client, output_file_id=output_file_id
        )
        return page_responses


async def _wait_and_create_prompt_responses(
    batch: UploadBatch, client: OpenAIClient, semaphore: asyncio.Semaphore
):
    async with semaphore:
        logger.info(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        output_file_id = await wait_for_batch_completion(client=client, batch=batch)
        logger.info(f"batch completed - {batch.id} - {batch.trace_id}")
        prompt_responses = await process_prompts_output(
            client=client, output_file_id=output_file_id
        )
        return prompt_responses


async def _create_batch_jobs(
    batch_file: BatchFile,
    client: OpenAIClient,
    trace_id: UUID,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
        upload_batch = await create_batch(
            client=client, file_id=batch_file.id, trace_id=trace_id
        )
        return upload_batch


async def _delete_associated_files(open_ai_client, remote_file_handler):
    for file in remote_file_handler.created_files:
        logger.info(f"deleting - {file}")
        await open_ai_client.delete_file(file)
