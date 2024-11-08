import uuid

import httpx


# TODO get from URL or from file system
async def add_file_to_temp_directory(pdf_source_url: str, temp_directory: str) -> str:
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", pdf_source_url) as response:
            response.raise_for_status()  # Check for HTTP errors
            content_type = response.headers.get("Content-Type")
            file_name = _determine_file_name(content_type)
            with open("/".join([temp_directory, file_name]), "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)
            return file_name


def _determine_file_name(content_type):
    # TODO custom errors
    # TODO other types besides pdf
    if content_type != "application/pdf":
        raise ValueError("Content-Type must be application/pdf")
    return f"{uuid.uuid4()}.pdf"
