import asyncio

from parallex.models.parallex_callable_input import ParallexCallableOutput
from parallex.parallex import parallex

def example_post_process(output: ParallexCallableOutput) -> None:
    print(f"Post-processing file: {output.file_name}")

if __name__ == '__main__':
    asyncio.run(parallex(
        pdf_source_url="https://summed-public.s3.us-west-2.amazonaws.com/medicare_plan_docs/2025/H0028-007-000/EOC.pdf",
        post_process_callable=example_post_process,
    ))

