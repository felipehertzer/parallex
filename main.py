import asyncio
import os
import time

from parallex.models.parallex_callable_output import ParallexCallableOutput
from parallex.parallex import parallex

os.environ["AZURE_OPENAI_API_KEY"] = "key"
os.environ["AZURE_OPENAI_ENDPOINT"] = "endpoint.com"
os.environ["AZURE_OPENAI_API_VERSION"] = "deployment_version"
os.environ["AZURE_OPENAI_API_DEPLOYMENT"] = "gpt-4o-global-batch" # this is the deployment name

model = "gpt-4o"

def example_post_process(output: ParallexCallableOutput) -> None:
    print(f"Post-processing file: {output.file_name}")
    for page in output.pages:
        print(f"Post-processing page: {page.page_number}")
    print(f"Post-processing file: {output.pages[0].output_content}")

    combined_content = "\n\n".join(page.output_content for page in output.pages)
    # Write to a Markdown file
    with open(f"{output.trace_id}-{output.file_name}.md", "w") as md_file:
        md_file.write(combined_content)


files = []

async def process_file(file_url: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        await parallex(
            model=model,
            pdf_source_url=file_url,
            post_process_callable=example_post_process,
        )

async def main():
    semaphore = asyncio.Semaphore(10)
    tasks = []
    for file_url in files:
        task = asyncio.create_task(process_file(file_url, semaphore))
        tasks.append(task)
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    start_time = time.time()
    asyncio.run(main())
    print("Execution time:", time.time() - start_time, "seconds")

