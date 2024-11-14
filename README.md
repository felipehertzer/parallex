# Parallex

### What it does
- Converts PDF into images
- Makes requests to Azure OpenAI to covert the images to markdown using Batch API
  - [Azure OpenAPI Batch](https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/batch?tabs=standard-input%2Cpython-secure&pivots=programming-language-python)
  - [OpenAPI Batch](https://platform.openai.com/docs/guides/batch)
- Polls for batch completion and then coverts AI responses in structured output based on the page of the corresponding PDF
- Post batch processing to do what you wish with the resulting markdown

### Requirements
Parallex uses `graphicsmagick` for the conversion of PDF to images. 
```bash
brew install graphicsmagick
```


### Example usage

```python
import os
from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.parallex import parallex

os.environ["AZURE_OPENAI_API_KEY"] = "key"
os.environ["AZURE_OPENAI_ENDPOINT"] = "your-endpoint.com"
os.environ["AZURE_OPENAI_API_VERSION"] = "deployment_version"
os.environ["AZURE_OPENAI_API_DEPLOYMENT"] = "deployment_name"

model = "gpt-4o"

async def some_operation(file_url: str) -> None:
  response_data: ParallexCallableOutput = await parallex(
    model=model,
    pdf_source_url=file_url,
    post_process_callable=example_post_process, # Optional
    concurrency=2, # Optional
    prompt_text="Turn images into markdown", # Optional
    log_level="ERROR" # Optional
  )
  pages = response_data.pages

def example_post_process(output: ParallexCallableOutput) -> None:
    file_name = output.file_name
    pages = output.pages
    for page in pages:
        markdown_for_page = page.output_content
        pdf_page_number = page.page_number
        
```

Responses have the following structure;
```python
class ParallexCallableOutput(BaseModel):
    file_name: str = Field(description="Name of file that is processed")
    pdf_source_url: str = Field(description="Given URL of the source of output")
    trace_id: UUID = Field(description="Unique trace for each file")
    pages: list[PageResponse] = Field(description="List of PageResponse objects")

class PageResponse(BaseModel):
    output_content: str = Field(description="Markdown generated for the page")
    page_number: int = Field(description="Page number of the associated PDF")
```

### Default prompt is 
```python
"""
    Convert the following PDF page to markdown.
    Return only the markdown with no explanation text.
    Leave out any page numbers and redundant headers or footers.
    Do not include any code blocks (e.g. "```markdown" or "```") in the response.
    If unable to parse, return an empty string.
"""
```
