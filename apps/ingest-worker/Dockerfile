FROM python:3.11-slim

WORKDIR /app

COPY apps/ingest-worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/ingest-worker /app

COPY apps/api/src /app/src

ENV PYTHONPATH=/app:/app/apps

COPY apps/shared /app/apps/shared

COPY apps/ingest-worker/config /app/config

CMD ["python", "-u", "main.py"]

