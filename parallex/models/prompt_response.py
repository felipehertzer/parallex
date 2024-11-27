from pydantic import BaseModel, Field


class PromptResponse(BaseModel):
    output_content: str = Field(description="Response from the model")
    prompt_index: int = Field(description="Index corresponding to the given prompts")
