import tempfile
from typing import Callable

from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.uploader import upload_image_for_processing
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.models.parallex_ouput import ParallexOutput


# TODO pdf_source_url: str change to be URL or path
# TODO post_process_callable
async def parallex(
        model: str,
        pdf_source_url: str,
        post_process_callable: Callable[..., None],
) -> ParallexOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
        raw_file = await add_file_to_temp_directory(
            pdf_source_url,
            temp_directory
        )
        print(f"inside parallex -- file_name: {raw_file.name}")


        # convert to Image Before passing to upload_image_for_processing
        # create file API call (WORKS)
        # open_ai_client = OpenAIClient(model=model)
        # batch_file = upload_image_for_processing(client=open_ai_client, raw_file=raw_file, temp_directory=temp_directory)
        # print(batch_file.model_dump())



        # Using file file-18b343beeab64da5a0e9b3d85cc3d845
        # batch API call



        # poll for results? Once AI responses ready, perform custom task?
        callable_output = ParallexCallableOutput(file_name=raw_file.name)
        post_process_callable(output=callable_output)
    return ParallexOutput(name="TODO")
