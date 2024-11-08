from pydantic import BaseModel, Field


class ParallexCallableOutput(BaseModel):
    file_name: str = Field(description="Name of file that is processed")
