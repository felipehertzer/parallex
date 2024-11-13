import os

from openai import AzureOpenAI
from openai._legacy_response import HttpxBinaryResponseContent
from openai.types import FileObject, Batch


# TODO init based on model not just azure
# Exceptions for missing keys, etc
class OpenAIClient:
    def __init__(self, model: str):
        self.model = model

        self._client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        )

    # TODO take in ImageFile
    def upload(self, file_path: str) -> FileObject:
        return self._client.files.create(
            file=open(file_path, "rb"),
            purpose="batch"
        )

    def create_batch(self, upload_file_id: str) -> Batch:
        return self._client.batches.create(
            input_file_id=upload_file_id,
            endpoint="/chat/completions", # TODO this could be configured see _jsonl_format
            completion_window="24h",
        )

    def retrieve_batch(self, batch_id: str) -> Batch:
        return self._client.batches.retrieve(batch_id)

    def retrieve_file(self, file_id: str) -> HttpxBinaryResponseContent:
        return self._client.files.content(file_id)
