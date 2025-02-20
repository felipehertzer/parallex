import uuid
from pathlib import Path
from typing import Union

import httpx

from parallex.file_management.utils import file_in_temp_dir
from parallex.models.raw_file import RawFile

ALLOWED_CONTENT_TYPES = {
    "application/pdf": "pdf",
    "image/jpeg": "jpg",
    "image/png": "png",
}


async def add_file_to_temp_directory(
    file_source: Union[str, Path], temp_directory: str
) -> RawFile:
    """Downloads file from URL or copies from file system and adds to temp directory"""
    file_trace_id = uuid.uuid4()

    if isinstance(file_source, str) and file_source.startswith(("http://", "https://")):
        return await _download_file(file_source, temp_directory, file_trace_id)
    elif isinstance(file_source, (str, Path)):
        return _copy_local_file(file_source, temp_directory, file_trace_id)
    else:
        raise ValueError("Invalid file source. Must be a URL or a file path.")


async def _download_file(
    url: str, temp_directory: str, file_trace_id: uuid.UUID
) -> RawFile:
    given_file_name = url.split("/")[-1]
    async with httpx.AsyncClient() as client:
        try:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type")
                file_name = _determine_file_name(file_trace_id, content_type)
                path = file_in_temp_dir(temp_directory, file_name)
                with open(path, "wb") as file:
                    async for chunk in response.aiter_bytes():
                        file.write(chunk)

                return RawFile(
                    name=file_name,
                    path=path,
                    content_type=content_type,
                    given_name=given_file_name,
                    pdf_source_url=url,
                    trace_id=file_trace_id,
                )
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error occurred: {e}")
        except httpx.RequestError as e:
            raise Exception(f"An error occurred while requesting the file: {e}")


def _copy_local_file(
    file_path: Union[str, Path], temp_directory: str, file_trace_id: uuid.UUID
) -> RawFile:
    source_path = Path(file_path)
    if not source_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    content_type = _get_content_type(source_path)
    file_name = _determine_file_name(file_trace_id, content_type)
    destination_path = file_in_temp_dir(temp_directory, file_name)

    Path(destination_path).write_bytes(source_path.read_bytes())

    return RawFile(
        name=file_name,
        path=destination_path,
        content_type=content_type,
        given_name=source_path.name,
        pdf_source_url=None,
        trace_id=file_trace_id,
    )


def _get_content_type(file_path: Path) -> str:
    extension = file_path.suffix.lower()[1:]  # Remove the leading dot
    for content_type, ext in ALLOWED_CONTENT_TYPES.items():
        if ext == extension:
            return content_type
    raise ValueError(f"Unsupported file type: {extension}")


def _determine_file_name(file_trace_id: uuid.UUID, content_type: str) -> str:
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise ValueError(f"Unsupported Content-Type: {content_type}")

    extension = ALLOWED_CONTENT_TYPES[content_type]
    return f"{file_trace_id}.{extension}"
