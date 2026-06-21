FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV OUTPUT_DIR=/app/outputs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "sitemap_exchange_rate_processors.backend.connect_scraper_with_kafka:app", "--host", "0.0.0.0", "--port", "8000"]
