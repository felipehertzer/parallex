import asyncio
import tempfile
import logging
from typing import Callable

from parallex.ai.batch_processor import wait_for_batch_completion, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_output
from parallex.ai.uploader import upload_image_for_processing
from parallex.file_management.converter import convert_pdf_to_images
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.models.parallex_ouput import ParallexOutput


# TODO pdf_source_url: str change to be URL or path
# TODO post_process_callable
async def parallex(
        model: str,
        pdf_source_url: str,
        post_process_callable: Callable[..., None],
) -> ParallexOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
        open_ai_client = OpenAIClient(model=model)

        # Fetch PDF and add to temp dir
        raw_file = await add_file_to_temp_directory(
            pdf_source_url=pdf_source_url,
            temp_directory=temp_directory
        )
        trace_id = raw_file.trace_id
        print(f"inside parallex -- file_name: {raw_file.name}")

        # Convert PDF to image
        image_files = await convert_pdf_to_images(raw_file=raw_file, temp_directory=temp_directory)

        batch_tasks = []
        for image_file in image_files:
            batch_task = asyncio.create_task(
                _create_image_and_batch_job(
                    image_file=image_file,
                    client=open_ai_client,
                    temp_directory=temp_directory,
                    trace_id=trace_id
                )
            )
            batch_tasks.append(batch_task)
        batches = await asyncio.gather(*batch_tasks)

        print("batches done")
        print(len(batches))
        print("batches done")

        page_tasks = []
        for batch in batches:
            page_task = asyncio.create_task(
                _wait_and_create_pages(batch, open_ai_client)
            )
            page_tasks.append(page_task)
        pages = await asyncio.gather(*page_tasks)


        # TODO add combined version of MD to output
        # perform custom task as callable?
        callable_output = ParallexCallableOutput(
            file_name=raw_file.given_name,
            pdf_source_url=raw_file.pdf_source_url,
            trace_id=trace_id,
            pages=pages
        )
        post_process_callable(output=callable_output)
    return ParallexOutput(name="TODO")


async def _wait_and_create_pages(batch, open_ai_client):
    # batch_id = "batch_7b219b17-3b1f-4279-9ce3-cd05184f482d"
    output_file_id = await wait_for_batch_completion(client=open_ai_client, batch_id=batch.id)
    # output_file_id = "file-a940ba9f-48b0-4139-8266-3df3a7b982a5"
    print(f"output_file_id: {output_file_id}")
    page_response = process_output(
        client=open_ai_client,
        output_file_id=output_file_id,
        page_number=batch.page_number
    )
    print(f"page_response: {page_response.page_number}")
    return page_response


async def _create_image_and_batch_job(image_file, client, temp_directory, trace_id):
    batch_file = upload_image_for_processing(
        client=client,
        image_file=image_file,
        temp_directory=temp_directory
    )
    file_id = batch_file.id
    batch = await create_batch(client=client, file_id=file_id, trace_id=trace_id, page_number=image_file.page_number)

    print("creating batch")
    print(batch.model_dump())
    print("creating batch")


    return batch
