FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app:/app/src:/app/apps
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DEFAULT_TIMEOUT=180

# Системные зависимости
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends gcc g++ curl && \
    rm -rf /var/lib/apt/lists/*

# 1. Poetry для остальных зависимостей (без torch)
RUN pip install --no-cache-dir poetry

COPY apps/api/pyproject.toml /app/

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

# 2. Torch CPU-only ПОСЛЕ poetry (poetry не трогает torch)
RUN pip install --no-cache-dir \
    torch==2.2.2 \
    --index-url https://download.pytorch.org/whl/cpu

# 3. sentence-transformers (зависит от torch, ставится после)
RUN pip install --no-cache-dir "sentence-transformers==2.7.0"

COPY apps/api/src /app/src
COPY apps/shared /app/apps/shared

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
