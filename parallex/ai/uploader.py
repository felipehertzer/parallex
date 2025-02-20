import base64
import json
import os
from typing import Optional, List
from uuid import UUID

from openai.lib._pydantic import to_strict_json_schema
from pydantic import BaseModel

from parallex.ai.open_ai_client import OpenAIClient
from parallex.file_management.utils import file_in_temp_dir
from parallex.models.batch_file import BatchFile
from parallex.models.image_file import ImageFile
from parallex.utils.constants import CUSTOM_ID_DELINEATOR
from parallex.utils.logger import logger

MAX_FILE_SIZE = 180 * 1024 * 1024  # 180 MB in bytes. Limit for OpenAI is 200MB.
DEFAULT_TEMPERATURE = 0.0


async def upload_images_for_processing(
    client: OpenAIClient,
    image_files: List[ImageFile],
    temp_directory: str,
    prompt_text: str,
    model_name: str,
    response_model: Optional[type[BaseModel]] = None,
    temperature: float = DEFAULT_TEMPERATURE,
) -> List[BatchFile]:
    """Base64 encodes image, converts to expected jsonl format and uploads"""
    trace_id = image_files[0].trace_id
    current_index = 0
    batch_files: List[BatchFile] = []
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
            current_index += 1
            upload_file_location = await set_file_location(
                current_index, temp_directory, trace_id
            )

        try:
            with open(image_file.path, "rb") as image:
                base64_encoded_image = base64.b64encode(image.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"Error encoding image {image_file.path}: {e}")
            continue

        prompt_custom_id = (
            f"{image_file.trace_id}{CUSTOM_ID_DELINEATOR}{image_file.page_number}.jsonl"
        )
        jsonl = _image_jsonl_format(
            prompt_custom_id,
            base64_encoded_image,
            prompt_text,
            model_name,
            response_model,
            temperature,
        )
        try:
            with open(upload_file_location, "a") as jsonl_file:
                jsonl_file.write(json.dumps(jsonl) + "\n")
        except Exception as e:
            logger.error(f"Error writing to jsonl file {upload_file_location}: {e}")

    batch_file = await _create_batch_file(client, trace_id, upload_file_location)
    batch_files.append(batch_file)
    return batch_files


async def upload_prompts_for_processing(
    client: OpenAIClient,
    prompts: List[str],
    temp_directory: str,
    trace_id: UUID,
    model_name: str,
    response_model: Optional[type[BaseModel]] = None,
    temperature: float = DEFAULT_TEMPERATURE,
) -> List[BatchFile]:
    """Creates jsonl file and uploads for processing"""
    current_index = 0
    batch_files: List[BatchFile] = []

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
            current_index += 1
            upload_file_location = await set_file_location(
                current_index, temp_directory, trace_id
            )

        prompt_custom_id = f"{trace_id}{CUSTOM_ID_DELINEATOR}{index}.jsonl"
        jsonl = _simple_jsonl_format(
            prompt_custom_id, prompt, model_name, response_model, temperature
        )
        try:
            with open(upload_file_location, "a") as jsonl_file:
                jsonl_file.write(json.dumps(jsonl) + "\n")
        except Exception as e:
            logger.error(f"Error writing to jsonl file {upload_file_location}: {e}")

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
    try:
        return (
            os.path.exists(upload_file_location)
            and os.path.getsize(upload_file_location) > MAX_FILE_SIZE
        )
    except Exception as e:
        logger.error(
            f"Error checking file size for {upload_file_location}: {e}. Assuming limit is reached."
        )
        return True  # Assume limit reached to be safe


async def _create_batch_file(
    client: OpenAIClient, trace_id: UUID, upload_file_location: str
) -> BatchFile:
    try:
        file_response = await client.upload(upload_file_location)
        return BatchFile(
            id=file_response.id,
            name=file_response.filename,
            purpose=file_response.purpose,
            status=file_response.status,
            trace_id=trace_id,
        )
    except Exception as e:
        logger.error(f"Error creating batch file from {upload_file_location}: {e}")
        raise


def _response_format(model: type[BaseModel]) -> dict:
    schema = to_strict_json_schema(model)
    return {
        "type": "json_schema",
        "json_schema": {"name": model.__name__, "strict": True, "schema": schema},
    }


def _simple_jsonl_format(
    prompt_custom_id: str,
    prompt_text: str,
    model_name: str,
    response_model: Optional[type[BaseModel]],
    temperature: float,
) -> dict:
    payload = {
        "custom_id": prompt_custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model_name,
            "messages": [{"role": "user", "content": prompt_text}],
            "temperature": temperature,
        },
    }
    if response_model:
        payload["body"]["response_format"] = _response_format(response_model)
    return payload


def _image_jsonl_format(
    prompt_custom_id: str,
    encoded_image: str,
    prompt_text: str,
    model_name: str,
    response_model: Optional[type[BaseModel]],
    temperature: float,
) -> dict:
    payload = {
        "custom_id": prompt_custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model_name,
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
            "response_format": {"type": "json_object"},
            "temperature": temperature,
        },
    }
    if response_model:
        payload["body"]["response_format"] = _response_format(response_model)
    return payload
