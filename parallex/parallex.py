import asyncio
import tempfile
from pathlib import Path
import uuid
from typing import Callable, Optional, Union, List
from uuid import UUID

from pydantic import BaseModel
from openai import APIError

from parallex.ai.batch_processor import (
    wait_for_batch_completion,
    create_batch,
    BatchCreationError,
    BatchProcessingError,
)
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

# Define more specific types for callables
PostProcessCallable = Callable[[ParallexCallableOutput], None]
PromptsPostProcessCallable = Callable[[ParallexPromptsCallableOutput], None]

DEFAULT_TEMPERATURE = 0.0


async def parallex(
    model_name: str,
    pdf_source: Union[str, Path],
    post_process_callable: Optional[PostProcessCallable] = None,
    concurrency: Optional[int] = 20,
    prompt_text: Optional[str] = DEFAULT_PROMPT,
    log_level: Optional[str] = "ERROR",
    response_model: Optional[type[BaseModel]] = None,
    api_key_env_name: str = "OPENAI_API_KEY",
    temperature: float = DEFAULT_TEMPERATURE,
) -> ParallexCallableOutput | List[UploadBatch] | None:
    """
    Orchestrates the process of extracting information from a PDF document using OpenAI's API.

    Args:
        model_name: The name of the OpenAI model to use.
        pdf_source: URL or file path to the PDF document.
        post_process_callable: Optional callable for post-processing the output.
        concurrency: Maximum number of concurrent API requests.
        prompt_text: Default prompt text to use for image processing.
        log_level: Logging level.
        response_model: Pydantic model for structured output.
        api_key_env_name: The environment variable name containing the OpenAI API key.
        temperature: The temperature to use for the OpenAI API.

    Returns:
        ParallexCallableOutput: Processed output containing extracted information.
    """
    setup_logger(log_level)
    remote_file_handler = RemoteFileHandler()
    open_ai_client = OpenAIClient(
        remote_file_handler=remote_file_handler,
        api_key_env_name=api_key_env_name,
    )
    try:
        return await _execute(
            open_ai_client=open_ai_client,
            pdf_source=pdf_source,
            post_process_callable=post_process_callable,
            concurrency=concurrency,
            prompt_text=prompt_text,
            model_name=model_name,
            response_model=response_model,
            temperature=temperature,
        )
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
    finally:
        if post_process_callable is not None:
            await delete_associated_files(open_ai_client, remote_file_handler)


async def parallex_simple_prompts(
    model_name: str,
    prompts: List[str],
    post_process_callable: Optional[PromptsPostProcessCallable] = None,
    log_level: Optional[str] = "ERROR",
    concurrency: Optional[int] = 20,
    response_model: Optional[type[BaseModel]] = None,
    api_key_env_name: str = "OPENAI_API_KEY",
    temperature: float = DEFAULT_TEMPERATURE,
) -> ParallexPromptsCallableOutput | None:
    """
    Processes a list of prompts using OpenAI's API.

    Args:
        model_name: The name of the OpenAI model to use.
        prompts: List of prompt strings to process.
        post_process_callable: Optional callable for post-processing the output.
        log_level: Logging level.
        concurrency: Maximum number of concurrent API requests.
        response_model: Pydantic model for structured output.
        api_key_env_name: The environment variable name containing the OpenAI API key.
        temperature: The temperature to use for the OpenAI API.

    Returns:
        ParallexPromptsCallableOutput: Processed output containing responses to the prompts.
    """
    setup_logger(log_level)
    remote_file_handler = RemoteFileHandler()
    open_ai_client = OpenAIClient(
        remote_file_handler=remote_file_handler,
        api_key_env_name=api_key_env_name,
    )
    try:
        return await _prompts_execute(
            open_ai_client=open_ai_client,
            prompts=prompts,
            post_process_callable=post_process_callable,
            concurrency=concurrency,
            model_name=model_name,
            response_model=response_model,
            temperature=temperature,
        )
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
    finally:
        if post_process_callable is not None:
            await delete_associated_files(open_ai_client, remote_file_handler)


async def _prompts_execute(
    open_ai_client: OpenAIClient,
    prompts: List[str],
    model_name: str,
    post_process_callable: Optional[PromptsPostProcessCallable] = None,
    concurrency: Optional[int] = 20,
    response_model: Optional[type[BaseModel]] = None,
    temperature: float = DEFAULT_TEMPERATURE,
) -> ParallexPromptsCallableOutput | List[UploadBatch] | None:
    """
    Executes the prompt processing workflow.

    Args:
        open_ai_client: OpenAI client instance.
        prompts: List of prompts to process.
        model_name: The name of the OpenAI model to use.
        post_process_callable: Optional callable for post-processing the output.
        concurrency: Maximum number of concurrent API requests.
        response_model: Pydantic model for structured output.
        temperature: The temperature to use for the OpenAI API.

    Returns:
        ParallexPromptsCallableOutput: Processed output containing responses to the prompts.
    """
    with tempfile.TemporaryDirectory() as temp_directory:
        trace_id = uuid.uuid4()
        try:
            batch_files = await upload_prompts_for_processing(
                client=open_ai_client,
                prompts=prompts,
                temp_directory=temp_directory,
                trace_id=trace_id,
                model_name=model_name,
                response_model=response_model,
                temperature=temperature,
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

            if post_process_callable is None:
                return batch_jobs

            process_semaphore = asyncio.Semaphore(concurrency)
            prompt_tasks = []
            for batch in batch_jobs:
                logger.info(
                    f"waiting for batch to complete - {batch.id} - {batch.trace_id}"
                )
                prompt_task = asyncio.create_task(
                    wait_and_create_prompt_responses(
                        batch=batch,
                        client=open_ai_client,
                        semaphore=process_semaphore,
                        response_model=response_model,
                    )
                )
                prompt_tasks.append(prompt_task)
            prompt_response_groups = await asyncio.gather(*prompt_tasks)

            flat_responses = [
                response for batch in prompt_response_groups for response in batch
            ]

            sorted_responses = sorted(flat_responses, key=lambda x: x.prompt_index)

            callable_output = ParallexPromptsCallableOutput(
                original_prompts=prompts,
                trace_id=trace_id,
                responses=sorted_responses,
            )

            post_process_callable(output=callable_output)

            return callable_output
        except (BatchCreationError, BatchProcessingError, APIError) as e:
            logger.error(f"Error during prompt processing: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during prompt processing: {e}")
            raise


async def _execute(
    open_ai_client: OpenAIClient,
    pdf_source: Union[str, Path],
    model_name: str,
    post_process_callable: Optional[PostProcessCallable] = None,
    concurrency: Optional[int] = 20,
    prompt_text: Optional[str] = DEFAULT_PROMPT,
    response_model: Optional[type[BaseModel]] = None,
    temperature: float = DEFAULT_TEMPERATURE,
) -> ParallexCallableOutput | List[UploadBatch] | None:
    """
    Executes the core workflow of extracting information from a PDF document.

    Args:
        open_ai_client: OpenAI client instance.
        pdf_source: URL or file path to the PDF document.
        model_name: The name of the OpenAI model to use.
        post_process_callable: Optional callable for post-processing the output.
        concurrency: Maximum number of concurrent API requests.
        prompt_text: Default prompt text to use for image processing.
        response_model: Pydantic model for structured output.
        temperature: The temperature to use for the OpenAI API.

    Returns:
        ParallexCallableOutput: Processed output containing extracted information.
    """
    with tempfile.TemporaryDirectory() as temp_directory:
        try:
            raw_file = await add_file_to_temp_directory(
                file_source=pdf_source, temp_directory=temp_directory
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
                model_name=model_name,
                response_model=response_model,
                temperature=temperature,
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

            if post_process_callable is None:
                return batch_jobs

            pages_tasks = []
            process_semaphore = asyncio.Semaphore(concurrency)
            for batch in batch_jobs:
                page_task = asyncio.create_task(
                    wait_and_create_pages(
                        batch=batch,
                        client=open_ai_client,
                        semaphore=process_semaphore,
                        response_model=response_model,
                    )
                )
                pages_tasks.append(page_task)
            page_groups = await asyncio.gather(*pages_tasks)

            pages = [page for batch_pages in page_groups for page in batch_pages]
            logger.info(f"pages done. total pages- {len(pages)} - {trace_id}")
            sorted_pages = sorted(pages, key=lambda x: x.page_number)

            callable_output = ParallexCallableOutput(
                file_name=raw_file.given_name,
                pdf_source_url=raw_file.pdf_source_url,
                trace_id=trace_id,
                pages=sorted_pages,
            )

            post_process_callable(output=callable_output)

            return callable_output
        except (BatchCreationError, BatchProcessingError, APIError) as e:
            logger.error(f"Error during PDF processing: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during PDF processing: {e}")
            raise


async def wait_and_create_pages(
    batch: UploadBatch,
    client: OpenAIClient,
    semaphore: asyncio.Semaphore,
    response_model: Optional[type[BaseModel]] = None,
) -> List[BaseModel]:
    """
    Waits for a batch to complete and processes the output to create page responses.

    Args:
        batch: The batch to wait for.
        client: OpenAI client instance.
        semaphore: Semaphore to limit concurrency.
        response_model: Pydantic model for structured output.

    Returns:
        List: List of page responses.
    """
    async with semaphore:
        logger.info(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        try:
            output_file_id = await wait_for_batch_completion(client=client, batch=batch)
            logger.info(f"batch completed - {batch.id} - {batch.trace_id}")
            page_responses = await process_images_output(
                client=client,
                output_file_id=output_file_id,
                response_model=response_model,
            )
            return page_responses
        except (BatchProcessingError, APIError) as e:
            logger.error(f"Error processing batch {batch.id}: {e}")
            raise


async def wait_and_create_prompt_responses(
    batch: UploadBatch,
    client: OpenAIClient,
    semaphore: asyncio.Semaphore,
    response_model: Optional[type[BaseModel]] = None,
) -> List[BaseModel]:
    """
    Waits for a batch to complete and processes the output to create prompt responses.

    Args:
        batch: The batch to wait for.
        client: OpenAI client instance.
        semaphore: Semaphore to limit concurrency.
        response_model: Pydantic model for structured output.

    Returns:
        List: List of prompt responses.
    """
    async with semaphore:
        logger.info(f"waiting for batch to complete - {batch.id} - {batch.trace_id}")
        try:
            output_file_id = await wait_for_batch_completion(client=client, batch=batch)
            logger.info(f"batch completed - {batch.id} - {batch.trace_id}")
            prompt_responses = await process_prompts_output(
                client=client,
                output_file_id=output_file_id,
                response_model=response_model,
            )
            return prompt_responses
        except (BatchProcessingError, APIError) as e:
            logger.error(f"Error processing batch {batch.id}: {e}")
            raise


async def _create_batch_jobs(
    batch_file: BatchFile,
    client: OpenAIClient,
    trace_id: UUID,
    semaphore: asyncio.Semaphore,
) -> UploadBatch:
    """
    Creates a batch processing job.

    Args:
        batch_file: The batch file to process.
        client: OpenAI client instance.
        trace_id: Trace ID for tracking.
        semaphore: Semaphore to limit concurrency.

    Returns:
        UploadBatch: Information about the created batch.
    """
    async with semaphore:
        try:
            upload_batch = await create_batch(
                client=client, file_id=batch_file.id, trace_id=trace_id
            )
            return upload_batch
        except (BatchCreationError, APIError) as e:
            logger.error(f"Error creating batch for file {batch_file.id}: {e}")
            raise


async def delete_associated_files(
    open_ai_client: OpenAIClient, remote_file_handler: RemoteFileHandler
) -> None:
    """
    Deletes associated files from OpenAI.

    Args:
        open_ai_client: OpenAI client instance.
        remote_file_handler: Remote file handler instance.
    """
    for file in remote_file_handler.created_files:
        logger.info(f"deleting - {file}")
        try:
            await open_ai_client.delete_file(file)
        except APIError as e:  # Catch APIError specifically
            logger.warning(f"API error deleting file {file}: {e}")
        except Exception as e:  # Catch other exceptions
            logger.warning(f"Failed to delete file {file}: {e}")
