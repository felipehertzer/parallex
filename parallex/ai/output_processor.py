import json

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.page_response import PageResponse


async def process_output(
    client: OpenAIClient, output_file_id: str
) -> list[PageResponse]:
    """Gets content from completed Batch to create PageResponse with created markdown"""
    file_response = await client.retrieve_file(output_file_id)
    raw_responses = file_response.text.strip().split("\n")

    pages = []
    for raw_response in raw_responses:
        json_response = json.loads(raw_response)
        custom_id = json_response["custom_id"]
        page_number = custom_id.split("--page--")[1].split(".")[0]
        output_content = json_response["response"]["body"]["choices"][0]["message"]["content"]
        page = PageResponse(output_content=output_content, page_number=int(page_number))
        pages.append(page)
    return pages
