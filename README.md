# Parallex for OpenAI

### What it does
- Converts PDF into images
- Makes requests to OpenAI to convert the images to markdown using Batch API
  - [OpenAPI Batch](https://platform.openai.com/docs/guides/batch)
- Polls for batch completion and then converts AI responses in structured output based on the page of the corresponding PDF
- Post batch processing to do what you wish with the resulting markdown

### Requirements
Parallex uses `graphicsmagick` for the conversion of PDF to images. 
```bash
brew install graphicsmagick
```

### Installation
```bash
pip install parallex
```

### Example usage
```python
import os
from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.parallex import parallex

os.environ["OPENAI_API_KEY"] = "key"

model = "gpt-4o-mini"

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
    pdf_source_url: Optional[str] = Field(description="Given URL of the source of output")
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

### Batch processing for list of prompts
If you do not need to process images, but just want to process prompts using the Batch API,
you can call;
```python
response_data: ParallexPromptsCallableOutput = await parallex_simple_prompts(
    model=model,
    prompts=["Some prompt", "Some other prompt"],
    post_process_callable=example_post_process
)
responses = response_data.responses
```
This will create a batch that includes all the prompts in `prompts` and responses can be tied back to the prompt by index.

Responses have the following structure;
```python
class ParallexPromptsCallableOutput(BaseModel):
    original_prompts: list[str] = Field(description="List of given prompts")
    trace_id: UUID = Field(description="Unique trace for each file")
    responses: list[PromptResponse] = Field(description="List of PromptResponse objects")

class PromptResponse(BaseModel):
    output_content: str = Field(description="Response from the model")
    prompt_index: int = Field(description="Index corresponding to the given prompts")
```
