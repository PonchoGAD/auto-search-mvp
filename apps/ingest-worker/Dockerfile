FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir playwright sqlalchemy psycopg2-binary \
    && playwright install --with-deps chromium

COPY . /app

CMD ["python", "main.py"]
