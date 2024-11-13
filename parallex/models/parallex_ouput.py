from pydantic import BaseModel, Field


class ParallexOutput(BaseModel):
    name: str = Field(
        description="Name of the parallex output. Will be some info on call to batch llm"
    )
