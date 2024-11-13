import asyncio

from pdf2image import convert_from_path

from parallex.models.image_file import ImageFile
from parallex.models.raw_file import RawFile
from parallex.utils.logger import logger


async def convert_pdf_to_images(
    raw_file: RawFile, temp_directory: str
) -> list[ImageFile]:
    """Converts a PDF file to a series of images in the temp_directory. Returns a list ImageFile objects."""
    options = {
        "pdf_path": raw_file.path,
        "output_folder": temp_directory,
        "dpi": 300,
        "fmt": "png",
        "size": (None, 1056),
        "thread_count": 4,
        "use_pdftocairo": True,
        "paths_only": True,
    }

    try:
        image_paths = await asyncio.to_thread(convert_from_path, **options)
        return [
            ImageFile(
                path=path,
                trace_id=raw_file.trace_id,
                given_file_name=raw_file.given_name,
                page_number=(i + 1),
            )
            for i, path in enumerate(image_paths)
        ]
    except Exception as err:
        logger.error(f"Error converting PDF to images: {err}")
