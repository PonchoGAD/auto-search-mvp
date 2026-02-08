FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app/src

RUN pip install --no-cache-dir poetry

COPY apps/api/pyproject.toml /app/

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root
 

COPY apps/api/src /app/src

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
