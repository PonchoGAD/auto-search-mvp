FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY apps/ingest-worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/ingest-worker /app

COPY apps/api/src /app/src

ENV PYTHONPATH=/app:/app/apps

COPY apps/shared /app/apps/shared

COPY apps/ingest-worker/config /app/config

CMD ["python", "-u", "main.py"]

