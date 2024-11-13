from pydantic import BaseModel


# Need things like page: int, etc
class PageResponse(BaseModel):
    output_content: str
