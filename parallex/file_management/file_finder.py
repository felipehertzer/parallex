import uuid

import httpx

from parallex.models.raw_file import RawFile


# TODO get from URL or from file system
# TODO naming prefix passed along to this method to give user meaning to batches
async def add_file_to_temp_directory(pdf_source_url: str, temp_directory: str) -> RawFile:
    given_file_name = pdf_source_url.split('/')[-1]
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", pdf_source_url) as response:
            response.raise_for_status()  # Check for HTTP errors
            content_type = response.headers.get("Content-Type")
            file_name = _determine_file_name(given_file_name, content_type)
            path = "/".join([temp_directory, file_name])
            with open(path, "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)

            return RawFile(name=file_name, path=path, content_type=content_type)


def _determine_file_name(given_file_name: str, content_type: str):
    # TODO custom errors
    # TODO other types besides pdf
    name, extension = given_file_name.split('.')
    if "application/pdf" not in content_type:
        raise ValueError("Content-Type must be application/pdf")
    return f"{name}--{uuid.uuid4()}.pdf"
