from pydantic import BaseModel


class RawFile(BaseModel):
    name: str
    path: str
    content_type: str
