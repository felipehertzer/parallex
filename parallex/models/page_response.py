from pydantic import BaseModel, Field


# Need things like page: int, etc
class PageResponse(BaseModel):
    output_content: str = Field(description="Markdown generated for the page")
    page_number: int = Field(description="Page number of the associated PDF")
