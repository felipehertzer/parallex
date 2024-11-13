import tempfile
from typing import Callable

from parallex.ai.batch_processor import process_batch, create_batch
from parallex.ai.open_ai_client import OpenAIClient
from parallex.ai.output_processor import process_output
from parallex.ai.uploader import upload_image_for_processing
from parallex.file_management.file_finder import add_file_to_temp_directory
from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.models.parallex_ouput import ParallexOutput
from parallex.models.upload_batch import build_batch


# TODO pdf_source_url: str change to be URL or path
# TODO post_process_callable
async def parallex(
        model: str,
        pdf_source_url: str,
        post_process_callable: Callable[..., None],
) -> ParallexOutput:
    with tempfile.TemporaryDirectory() as temp_directory:
        open_ai_client = OpenAIClient(model=model)
        raw_file = await add_file_to_temp_directory(
            pdf_source_url,
            temp_directory
        )
        print(f"inside parallex -- file_name: {raw_file.name}")


        # convert to Image Before passing to upload_image_for_processing
        # create file API call (WORKS)
        batch_file = upload_image_for_processing(client=open_ai_client, raw_file=raw_file, temp_directory=temp_directory)
        print(batch_file.model_dump())
        file_id = batch_file.id




        # Using file file-18b343beeab64da5a0e9b3d85cc3d845
        # batch API call to create batch
        trace_id = raw_file.trace_id
        batch = await create_batch(client=open_ai_client, file_id=file_id, trace_id=trace_id)
        batch_id = batch.id

        #Track; batch; job; progress
        # Do stuff with batch using batch_7b219b17-3b1f-4279-9ce3-cd05184f482d
        # batch_id = "batch_7b219b17-3b1f-4279-9ce3-cd05184f482d"
        output_file_id = process_batch(client=open_ai_client, batch_id=batch_id)
        print(output_file_id)

        # Take output and do thing
        # output_file_id = "file-a940ba9f-48b0-4139-8266-3df3a7b982a5"
        page_output = process_output(client=open_ai_client, output_file_id=output_file_id)


        # poll for results? Once AI responses ready, perform custom task?
        callable_output = ParallexCallableOutput(file_name=raw_file.name, pages=[page_output])
        post_process_callable(output=callable_output)
    return ParallexOutput(name="TODO")
