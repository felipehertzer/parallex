import tempfile
from typing import Callable

from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.models.parallex_ouput import ParallexOutput


# TODO pdf_source_url: str change to be URL or path
# TODO post_process_callable
async def parallex(
        pdf_source_url: str,
        post_process_callable: Callable[..., None],
) -> ParallexOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
        file_name = await add_file_to_temp_directory(pdf_source_url, temp_directory)
        print(f"inside parallex -- file_name: {file_name}")
        # convert to Image
        # batch API call
        # poll for results? Once AI responses ready, perform custom task?
        callable_output = ParallexCallableOutput(file_name=file_name)
        post_process_callable(output=callable_output)
    return ParallexOutput(name="TODO")
