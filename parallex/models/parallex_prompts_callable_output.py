from uuid import UUID

from pydantic import BaseModel, Field

from parallex.models.prompt_response import PromptResponse


class ParallexPromptsCallableOutput(BaseModel):
    original_prompts: list[str] = Field(description="List of given prompts")
    trace_id: UUID = Field(description="Unique trace for each file")
    responses: list[PromptResponse] = Field(
        description="List of PromptResponse objects"
    )
