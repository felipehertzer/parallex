DEFAULT_PROMPT = """
    Convert the following PDF page to markdown.
    Return only the markdown with no explanation text.
    Leave out any page numbers and redundant headers or footers.
    Do not include any code blocks (e.g. "```markdown" or "```") in the response.
    If unable to parse, return an empty string.
    """

CUSTOM_ID_DELINEATOR = "--parallex--"
