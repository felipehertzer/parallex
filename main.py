import asyncio
import os
import time

from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.parallex import parallex

# Set environment variables (replace with your actual keys/endpoints)
os.environ["OPENAI_API_KEY"] = "YOUR_OPENAI_API_KEY"

model_name = "gpt-4o-mini"  # Or any other suitable OpenAI model

files = [
    "https://example.com/sample1.pdf",  # Replace with actual PDF URLs/Paths
    "sample2.pdf",
]


def example_post_process(output: ParallexCallableOutput) -> None:
    """Example post-processing function."""
    file_name = output.file_name
    pages = output.pages
    for page in pages:
        markdown_for_page = page.output_content
        pdf_page_number = page.page_number
        print(
            f"Processed page {pdf_page_number} of {file_name}: {markdown_for_page[:50]}..."
        )  # Print a snippet


async def process_file(file_url: str, semaphore: asyncio.Semaphore):
    """Processes a single PDF file using Parallex."""
    async with semaphore:
        try:
            output = await parallex(
                model_name=model_name,
                pdf_source=file_url,
                post_process_callable=example_post_process,
            )
            if output:
                print(f"Successfully processed file: {file_url}")
            else:
                print(f"File processing returned None: {file_url}")
        except Exception as e:
            print(f"Error processing file {file_url}: {e}")


async def main():
    """Main function to process multiple files concurrently."""
    semaphore = asyncio.Semaphore(10)  # Limit concurrent tasks
    tasks = []
    for file_url in files:
        task = asyncio.create_task(process_file(file_url, semaphore))
        tasks.append(task)
    await asyncio.gather(*tasks)  # Run all tasks concurrently


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print("Execution time:", time.time() - start_time, "seconds")
