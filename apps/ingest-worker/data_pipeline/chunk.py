import re


def chunk_text(text: str, chunk_size: int = 800):

    if not text:
        return []

    sentences = re.split(r'(?<=[.!?])\s+', text)

    chunks = []

    current = ""

    for s in sentences:

        if len(current) + len(s) < chunk_size:
            current += " " + s

        else:
            chunks.append(current.strip())
            current = s

    if current:
        chunks.append(current.strip())

    return chunks