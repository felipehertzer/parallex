import base64
import json
import os

from parallex.ai.open_ai_client import OpenAIClient
from parallex.file_management.utils import file_in_temp_dir
from parallex.models.batch_file import BatchFile
from parallex.models.image_file import ImageFile


async def upload_image_for_processing(
    client: OpenAIClient,
    image_file: ImageFile,
    temp_directory: str,
):
    """Base64 encodes image, converts to expected jsonl format and uploads to create a Batch"""
    with open(image_file.path, "rb") as image:
        base64_encoded_image = base64.b64encode(image.read()).decode("utf-8")

    upload_file_name = f"{image_file.trace_id}--page--{image_file.page_number}.jsonl"
    jsonl = _jsonl_format(upload_file_name, base64_encoded_image)
    upload_file_location = file_in_temp_dir(
        directory=temp_directory, file_name=upload_file_name
    )
    with open(upload_file_location, "w") as jsonl_file:
        jsonl_file.write(json.dumps(jsonl) + "\n")

    file_response = await client.upload(upload_file_location)
    return BatchFile(
        id=file_response.id,
        name=file_response.filename,
        purpose=file_response.purpose,
        status=file_response.status,
        trace_id=image_file.trace_id,
    )


# TODO fine tune this and allow custom _prompt_text
def _jsonl_format(upload_file_name: str, encoded_image: str):
    return {
        "custom_id": upload_file_name,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": os.getenv("AZURE_OPENAI_API_DEPLOYMENT"),
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": _prompt_text()},
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


def _prompt_text():
    return """
    Convert the following PDF page to markdown.
    Return only the markdown with no explanation text.
    Leave out any page numbers and redundant headers or footers.
    Do not include any code blocks (e.g. "```markdown" or "```") in the response.
    If unable to parse, return an empty string.
    """
