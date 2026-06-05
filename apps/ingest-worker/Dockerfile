FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libxkbcommon0 libxcomposite1 libxrandr2 libgbm1 \
    libasound2 libpangocairo-1.0-0 libpango-1.0-0 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

COPY apps/ingest-worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps chromium

COPY apps/ingest-worker /app

COPY apps/api/src /app/src

ENV PYTHONPATH=/app:/app/apps

COPY apps/shared /app/apps/shared

COPY apps/ingest-worker/config /app/config

CMD ["python", "-u", "main.py"]

