FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY apps/bot_api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/bot_api/src ./src
COPY apps/bot_api/alembic ./alembic
COPY apps/bot_api/alembic.ini ./alembic.ini

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8100"]