import json
from typing import TypeVar, Callable, Optional, List

from pydantic import BaseModel
from openai import APIError

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.page_response import PageResponse
from parallex.models.prompt_response import PromptResponse
from parallex.utils.constants import CUSTOM_ID_DELINEATOR
from parallex.utils.logger import logger


async def process_images_output(
    client: OpenAIClient,
    output_file_id: str,
    response_model: Optional[type[BaseModel]] = None,
) -> List[PageResponse]:
    """Processes the output file from an image processing batch job."""
    return await _process_output(
        client=client,
        output_file_id=output_file_id,
        response_model=response_model,
        response_builder=lambda content, identifier: PageResponse(
            output_content=content, page_number=int(identifier)
        ),
    )


async def process_prompts_output(
    client: OpenAIClient,
    output_file_id: str,
    response_model: Optional[type[BaseModel]] = None,
) -> List[PromptResponse]:
    """Processes the output file from a prompt processing batch job."""
    return await _process_output(
        client=client,
        output_file_id=output_file_id,
        response_model=response_model,
        response_builder=lambda content, identifier: PromptResponse(
            output_content=content, prompt_index=int(identifier)
        ),
    )


ResponseType = TypeVar("ResponseType")


async def _process_output(
    client: OpenAIClient,
    output_file_id: str,
    response_model: Optional[type[BaseModel]],
    response_builder: Callable[[str, str], ResponseType],
) -> List[ResponseType]:
    """
    Retrieves and processes the output file content, creating a list of response objects.

    Args:
        client: OpenAIClient instance.
        output_file_id: The ID of the output file to retrieve.
        response_model: An optional Pydantic model to parse the output content.
        response_builder: A callable that builds the response object from the content and identifier.

    Returns:
        A list of response objects.
    """
    try:
        file_response = await client.retrieve_file(output_file_id)
        raw_responses = file_response.text.strip().split("\n")
        responses: List[ResponseType] = []

        for raw_response in raw_responses:
            try:
                json_response = json.loads(raw_response)
                custom_id = json_response["custom_id"]
                identifier = custom_id.split(CUSTOM_ID_DELINEATOR)[1].split(".")[0]
                output_content = json_response["response"]["body"]["choices"][0][
                    "message"
                ]["content"]

                if response_model:
                    try:
                        json_data = json.loads(output_content)
                        output_content = response_model(**json_data)
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"Error parsing output content into model: {e}")
                        continue  # Skip this response if parsing fails

                response = response_builder(output_content, identifier)
                responses.append(response)
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error processing raw response: {e}")
                continue  # Skip this response if processing fails

        return responses

    except APIError as e:
        logger.error(f"API error while retrieving or processing file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while processing output: {e}")
        raise
