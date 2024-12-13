import base64
import json
import os
from uuid import UUID

from parallex.ai.open_ai_client import OpenAIClient
from parallex.file_management.utils import file_in_temp_dir
from parallex.models.batch_file import BatchFile
from parallex.models.image_file import ImageFile
from parallex.utils.constants import CUSTOM_ID_DELINEATOR

MAX_FILE_SIZE = 180 * 1024 * 1024  # 180 MB in bytes. Limit for Azure is 200MB.


async def upload_images_for_processing(
    client: OpenAIClient,
    image_files: list[ImageFile],
    temp_directory: str,
    prompt_text: str,
) -> list[BatchFile]:
    """Base64 encodes image, converts to expected jsonl format and uploads"""
    trace_id = image_files[0].trace_id
    current_index = 0
    batch_files = []
    upload_file_location = file_in_temp_dir(
        directory=temp_directory, file_name=f"{trace_id}-{current_index}.jsonl"
    )

    for image_file in image_files:
        if await _approaching_file_size_limit(upload_file_location):
            """When approaching upload file limit, upload and start new file"""
            batch_file = await _create_batch_file(
                client, trace_id, upload_file_location
            )
            batch_files.append(batch_file)
            upload_file_location = await _increment_batch_file_index(
                current_index, temp_directory, trace_id
            )

        with open(image_file.path, "rb") as image:
            base64_encoded_image = base64.b64encode(image.read()).decode("utf-8")

        prompt_custom_id = (
            f"{image_file.trace_id}{CUSTOM_ID_DELINEATOR}{image_file.page_number}.jsonl"
        )
        jsonl = _image_jsonl_format(prompt_custom_id, base64_encoded_image, prompt_text)
        with open(upload_file_location, "a") as jsonl_file:
            jsonl_file.write(json.dumps(jsonl) + "\n")
    batch_file = await _create_batch_file(client, trace_id, upload_file_location)
    batch_files.append(batch_file)
    return batch_files


async def upload_prompts_for_processing(
    client: OpenAIClient, prompts: list[str], temp_directory: str, trace_id: UUID
) -> list[BatchFile]:
    """Creates jsonl file and uploads for processing"""
    current_index = 0
    batch_files = []

    upload_file_location = await set_file_location(
        current_index, temp_directory, trace_id
    )
    for index, prompt in enumerate(prompts):
        if await _approaching_file_size_limit(upload_file_location):
            """When approaching upload file limit, upload and start new file"""
            batch_file = await _create_batch_file(
                client, trace_id, upload_file_location
            )
            batch_files.append(batch_file)
            upload_file_location = await _increment_batch_file_index(
                current_index, temp_directory, trace_id
            )

        prompt_custom_id = f"{trace_id}{CUSTOM_ID_DELINEATOR}{index}.jsonl"
        jsonl = _simple_jsonl_format(prompt_custom_id, prompt)
        with open(upload_file_location, "a") as jsonl_file:
            jsonl_file.write(json.dumps(jsonl) + "\n")
    batch_file = await _create_batch_file(client, trace_id, upload_file_location)
    batch_files.append(batch_file)
    return batch_files


async def set_file_location(
    current_index: int, temp_directory: str, trace_id: UUID
) -> str:
    return file_in_temp_dir(
        directory=temp_directory, file_name=f"{trace_id}-{current_index}.jsonl"
    )


async def _approaching_file_size_limit(upload_file_location: str) -> bool:
    return (
        os.path.exists(upload_file_location)
        and os.path.getsize(upload_file_location) > MAX_FILE_SIZE
    )


async def _increment_batch_file_index(
    current_index: int, temp_directory: str, trace_id: UUID
) -> str:
    current_index += 1
    upload_file_location = await set_file_location(
        current_index, temp_directory, trace_id
    )
    return upload_file_location


async def _create_batch_file(
    client: OpenAIClient, trace_id: UUID, upload_file_location: str
) -> BatchFile:
    file_response = await client.upload(upload_file_location)
    return BatchFile(
        id=file_response.id,
        name=file_response.filename,
        purpose=file_response.purpose,
        status=file_response.status,
        trace_id=trace_id,
    )


def _simple_jsonl_format(prompt_custom_id: str, prompt_text: str) -> dict:
    return {
        "custom_id": prompt_custom_id,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": os.getenv("AZURE_API_DEPLOYMENT"),
            "messages": [{"role": "user", "content": prompt_text}],
        },
    }


def _image_jsonl_format(prompt_custom_id: str, encoded_image: str, prompt_text: str):
    return {
        "custom_id": prompt_custom_id,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": os.getenv("AZURE_API_DEPLOYMENT"),
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt_text},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{encoded_image}"
                            },
                        },
                    ],
                }
            ],
            "max_tokens": 2000,
        },
    }
