FROM python:3.11-slim

WORKDIR /app

# =========================
# SYSTEM DEPENDENCIES
# =========================
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    ca-certificates \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# =========================
# PYTHON DEPS
# =========================
COPY apps/ingest-worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# =========================
# PLAYWRIGHT (WORKER ONLY)
# =========================
RUN playwright install --with-deps chromium

# =========================
# COPY SOURCE CODE
# =========================

# shared db (ВАЖНО)
COPY apps/shared /app/shared

# ingest-worker code
COPY apps/ingest-worker /app

# =========================
# PYTHONPATH
# =========================
ENV PYTHONPATH=/app

# =========================
# RUN
# =========================
CMD ["python", "main.py"]
