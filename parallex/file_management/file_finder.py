import uuid

import httpx

from parallex.file_management.utils import file_in_temp_dir
from parallex.models.raw_file import RawFile


# TODO get from URL or from file system
async def add_file_to_temp_directory(
    pdf_source_url: str, temp_directory: str
) -> RawFile:
    """Downloads file and adds to temp directory"""
    given_file_name = pdf_source_url.split("/")[-1]
    file_trace_id = uuid.uuid4()
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", pdf_source_url) as response:
            response.raise_for_status()  # Check for HTTP errors
            content_type = response.headers.get("Content-Type")
            file_name = _determine_file_name(
                given_file_name, file_trace_id, content_type
            )
            path = file_in_temp_dir(temp_directory, file_name)
            with open(path, "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)

            return RawFile(
                name=file_name,
                path=path,
                content_type=content_type,
                given_name=given_file_name,
                pdf_source_url=pdf_source_url,
                trace_id=file_trace_id,
            )


def _determine_file_name(given_file_name: str, file_trace_id, content_type: str):
    # TODO custom errors
    # TODO other types besides pdf
    name, extension = given_file_name.split(".")
    if "application/pdf" not in content_type:
        raise ValueError("Content-Type must be application/pdf")
    return f"{file_trace_id}.{extension}"
