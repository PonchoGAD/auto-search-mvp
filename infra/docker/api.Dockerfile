FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app:/app/src:/app/apps

RUN pip install --no-cache-dir poetry

COPY apps/api/pyproject.toml /app/

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root
 

COPY apps/api/src /app/src

COPY apps/shared /app/apps/shared

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
