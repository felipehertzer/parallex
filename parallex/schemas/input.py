from pydantic import BaseModel, Field

class Input(BaseModel):
    name: str
