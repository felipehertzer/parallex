import os
from openai import AsyncOpenAI
from openai._legacy_response import HttpxBinaryResponseContent
from openai.types import FileObject, FileDeleted, Batch

from parallex.file_management.remote_file_handler import RemoteFileHandler
from parallex.utils.logger import logger


class OpenAIClient:
    def __init__(
        self,
        remote_file_handler: RemoteFileHandler,
        api_key_env_name: str,
    ):
        self.file_handler = remote_file_handler

        self._client = AsyncOpenAI(
            api_key=os.getenv(api_key_env_name),
        )

    async def upload(self, file_path: str) -> FileObject:
        file = await self._client.files.create(
            file=open(file_path, "rb"), purpose="batch"
        )
        self.file_handler.add_file(file.id)
        return file

    async def create_batch(self, upload_file_id: str) -> Batch:
        batch = await self._client.batches.create(
            input_file_id=upload_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        self.file_handler.add_file(batch.input_file_id)
        self.file_handler.add_file(batch.output_file_id)
        self.file_handler.add_file(batch.error_file_id)
        return batch

    async def retrieve_batch(self, batch_id: str) -> Batch:
        batch = await self._client.batches.retrieve(batch_id)
        self.file_handler.add_file(batch.input_file_id)
        self.file_handler.add_file(batch.output_file_id)
        self.file_handler.add_file(batch.error_file_id)
        return batch

    async def retrieve_file(self, file_id: str) -> HttpxBinaryResponseContent:
        return await self._client.files.content(file_id)

    async def delete_file(self, file_id: str) -> FileDeleted | None:
        try:
            return await self._client.files.delete(file_id)
        except Exception as e:
            logger.info(f"Did not delete file: {e}")
