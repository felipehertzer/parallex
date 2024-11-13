import json

from parallex.ai.open_ai_client import OpenAIClient
from parallex.models.page_response import PageResponse


def process_output(client: OpenAIClient, output_file_id: str, page_number: int) -> PageResponse:
    file_response = client.retrieve_file(output_file_id)
    raw_responses = file_response.text.strip().split('\n')
    # TODO  There should just be one response right?

    contents = []
    for raw_response in raw_responses:
        json_response = json.loads(raw_response)
        # TODO handle better
        contents.append(json_response['response']['body']['choices'][0]['message']['content'])

    # TODO may need to get page number from json_response["custom_id"]
    return PageResponse(output_content="\r".join(contents), page_number=page_number)
