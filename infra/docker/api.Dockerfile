FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY apps/api/pyproject.toml /app/

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root
 
RUN playwright install --with-deps chromium

COPY apps/api/src /app/src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
