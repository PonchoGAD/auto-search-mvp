FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH=/app:/app/src:/app/apps
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DEFAULT_TIMEOUT=120

# Системные зависимости
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends \
        gcc g++ curl && \
    rm -rf /var/lib/apt/lists/*

# 1. Torch CPU-only (~800MB вместо 2.5GB с CUDA)
RUN pip install --no-cache-dir \
    torch==2.2.2 \
    --index-url https://download.pytorch.org/whl/cpu

# 2. Остальные зависимости (poetry исключает torch — уже установлен)
RUN pip install --no-cache-dir poetry

COPY apps/api/pyproject.toml /app/

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

COPY apps/api/src /app/src
COPY apps/shared /app/apps/shared

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
