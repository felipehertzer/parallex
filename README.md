# Parallex

### What it does
- Converts file into images
- Makes requests to OpenAI to covert the images to markdown
  - [Azure OpenAPI Batch](https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/batch?tabs=standard-input%2Cpython-secure&pivots=programming-language-python)
  - [OpenAPI Batch](https://platform.openai.com/docs/guides/batch)
- Post batch processing to do what you wish with the resulting markdown


# Notes for us as we build
### Poetry
- Using [poetry](https://python-poetry.org/docs/) for dependency management
- add dependency `poetry add pydantic`
- add dev dependency `poetry add --group dev black`
- run main script `poetry run python main.py`
- run dev commands `poetry run black parallex`


# General behavior
- parallex takes args to do things with file
- parallex takes args to specify llm model
- parallex takes a callable to execute once batch process is "ready"
