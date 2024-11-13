import os

from openai import AzureOpenAI
from openai.types import FileObject


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
