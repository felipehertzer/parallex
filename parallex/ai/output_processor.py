import json
from typing import TypeVar, Callable, Optional

from pydantic import BaseModel

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.page_response import PageResponse
from parallex.models.prompt_response import PromptResponse
from parallex.utils.constants import CUSTOM_ID_DELINEATOR


async def process_images_output(
    client: OpenAIClient, output_file_id: str, model: Optional[type[BaseModel]] = None
) -> list[PageResponse]:
    return await _process_output(
        client,
        output_file_id,
        model,
        lambda content, identifier: PageResponse(
            output_content=content, page_number=int(identifier)
        ),
    )


async def process_prompts_output(
    client: OpenAIClient, output_file_id: str, model: Optional[type[BaseModel]] = None
) -> list[PromptResponse]:
    """Gets content from completed Batch to create PromptResponse with LLM answers to given prompts"""
    return await _process_output(
        client,
        output_file_id,
        model,
        lambda content, identifier: PromptResponse(
            output_content=content, prompt_index=int(identifier)
        ),
    )


ResponseType = TypeVar("ResponseType")


async def _process_output(
    client: OpenAIClient,
    output_file_id: str,
    model: Optional[type[BaseModel]],
    response_builder: Callable[[str, str], ResponseType],
) -> list[ResponseType]:
    file_response = await client.retrieve_file(output_file_id)
    raw_responses = file_response.text.strip().split("\n")
    responses = []

    for raw_response in raw_responses:
        json_response = json.loads(raw_response)
        custom_id = json_response["custom_id"]
        identifier = custom_id.split(CUSTOM_ID_DELINEATOR)[1].split(".")[0]
        output_content = json_response["response"]["body"]["choices"][0]["message"]["content"]
        if model:
            json_data = json.loads(output_content)
            output_content = model(**json_data)
        response = response_builder(output_content, identifier)
        responses.append(response)

    return responses
