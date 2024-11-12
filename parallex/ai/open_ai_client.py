import os

from openai import AzureOpenAI


# TODO init based on model not just azure
# Exceptions for missing keys, etc
class OpenAPClient:
    def __init__(self, model: str):
        self.model = model

        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        )

    def complete(self):
        return self.client.chat.completions.create(model=self.model, messages=[{"role": "system", "content": "You are a helpful assistant."}, ])
