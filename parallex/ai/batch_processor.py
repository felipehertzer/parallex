import time
import datetime

from parallex.ai.open_ai_client import OpenAIClient


def process_batch(client: OpenAIClient, batch_id) -> str:
    # TODO pass in UploadBatch and mutate?
    # How to process? FIFO?
    # TODO handle "failed", "canceled"
    status = "validating"
    while status not in ("completed", "failed", "canceled"):
        time.sleep(5)
        batch_response = client.retrieve_batch(batch_id)
        status = batch_response.status
        if status == "completed":
            return batch_response.output_file_id
