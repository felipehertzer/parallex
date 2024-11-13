import base64
import json
import os

from parallex.ai.open_ai_client import OpenAIClient
from parallex.file_management.utils import file_in_temp_dir
from parallex.models.batch_file import BatchFile
from parallex.models.raw_file import RawFile


def upload_image_for_processing(client: OpenAIClient, raw_file: RawFile, temp_directory: str):
    image_path = os.path.join(os.path.dirname(__file__), "../pdf_screenshot.png")
    image_path = os.path.abspath(image_path)

    with open(image_path, "rb") as image_file:
        base64_encoded_image = base64.b64encode(image_file.read()).decode("utf-8")
    jsonl = _jsonl_format(raw_file, base64_encoded_image)

    upload_file_name = f"{raw_file.trace_id}.jsonl"
    upload_file_location = file_in_temp_dir(directory=temp_directory, file_name=upload_file_name)
    with(open(upload_file_location, "w")) as jsonl_file:
        jsonl_file.write(json.dumps(jsonl) + "\n")

    file_response = client.upload(upload_file_location)
    return BatchFile(
        id=file_response.id,
        name=file_response.filename,
        purpose=file_response.purpose,
        status=file_response.status,
        trace_id=raw_file.trace_id,
    )


# TODO fine tune this
def _jsonl_format(file, encoded_image):
    return {
        "custom_id": file.name,
        "method": "POST",
        "url": "/chat/completions",
        "body": {
            "model": os.getenv("AZURE_OPENAI_API_DEPLOYMENT"),
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": _prompt_text()
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{encoded_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 2000
        }
    }


def _prompt_text():
    return """
    Convert the following PDF page to markdown.
    Return only the markdown with no explanation text.
    Leave out any page numbers and redundant headers or footers.
    Do not include any code blocks (e.g. "```markdown" or "```") in the response.
    If unable to parse, return an empty string.
    """
