"""FastAPI app that triggers the scraper-to-Kafka Delta Nexus pipeline."""

import asyncio
import os
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, status
from pydantic import AnyHttpUrl, BaseModel, Field

from sitemap_exchange_rate_processors.broker import (
    FX_TOPIC,
    PRICE_TOPIC,
    KafkaProducerClient,
)
from sitemap_exchange_rate_processors.scrapers import AsyncCurrencyFetcher, AsyncPriceScraper
from sitemap_exchange_rate_processors.scrapers.compare_sitemap_exchange_rates import (
    DEFAULT_BASE_CURRENCY,
    DEFAULT_CURRENCY_URL as ENGINE_DEFAULT_CURRENCY_URL,
    run_currency_task,
    run_price_task,
)


DEFAULT_CURRENCY_URL = os.getenv("CURRENCY_URL", ENGINE_DEFAULT_CURRENCY_URL)
DEFAULT_KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

app = FastAPI(
    title="Delta Nexus Scraper Kafka API",
    version="1.0.0",
    description=(
        "API layer that triggers product and currency scraping, then publishes "
        "validated events to Kafka."
    ),
)


class PipelineRunRequest(BaseModel):
    """Request body for one scraper-to-Kafka pipeline run."""

    price_url: AnyHttpUrl = Field(
        ...,
        description="XML product feed or sitemap URL to scrape.",
    )
    currency_url: AnyHttpUrl = Field(
        default=DEFAULT_CURRENCY_URL,
        description="FloatRates-style currency page URL.",
    )
    kafka_servers: str = Field(
        default=DEFAULT_KAFKA_SERVERS,
        min_length=1,
        description="Kafka bootstrap server list.",
    )
    price_topic: str = Field(
        default=PRICE_TOPIC,
        min_length=1,
        description="Kafka topic for product price events.",
    )
    rate_topic: str = Field(
        default=FX_TOPIC,
        min_length=1,
        description="Kafka topic for exchange-rate events.",
    )
    base_currency: str = Field(
        default=DEFAULT_BASE_CURRENCY,
        min_length=3,
        max_length=3,
        description="Base currency used for the exchange-rate snapshot.",
    )


class PipelineRunResponse(BaseModel):
    """Response returned after a pipeline run completes."""

    status: str
    message: str
    price_url: str
    currency_url: str
    kafka_servers: str
    price_topic: str
    rate_topic: str
    product_records_published: int
    currency_records_published: int


class HealthResponse(BaseModel):
    """Basic service health response."""

    status: str
    service: str


@app.get("/", response_model=dict[str, str])
async def root() -> dict[str, str]:
    """Return a tiny service index."""
    return {
        "service": "Delta Nexus Scraper Kafka API",
        "docs": "/docs",
        "health": "/health",
        "run_pipeline": "/pipeline/run",
    }


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Confirm that the FastAPI process is running."""
    return HealthResponse(
        status="ok",
        service="delta-nexus-scraper-kafka-api",
    )


@app.post(
    "/pipeline/run",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_200_OK,
)
async def run_pipeline(request: PipelineRunRequest) -> PipelineRunResponse:
    """
    Run one scrape-and-publish job.

    The API fetches product data, fetches one exchange-rate snapshot, and
    publishes both record types to Kafka. CSV writing is handled by a Kafka
    consumer, not by this endpoint.
    """
    producer = KafkaProducerClient(
        bootstrap_servers=request.kafka_servers,
        price_topic=request.price_topic,
        rate_topic=request.rate_topic,
    )

    try:
        await producer.connect()
        price_records, rate_records = await asyncio.gather(
            run_price_task(AsyncPriceScraper(), str(request.price_url), producer),
            run_currency_task(
                AsyncCurrencyFetcher(str(request.currency_url)),
                request.base_currency.upper(),
                producer,
            ),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Pipeline failed while scraping or publishing to Kafka.",
                "error": str(exc),
            },
        ) from exc
    finally:
        await producer.disconnect()

    return PipelineRunResponse(
        status="completed",
        message="Scraped product and currency data, then published events to Kafka.",
        price_url=str(request.price_url),
        currency_url=str(request.currency_url),
        kafka_servers=request.kafka_servers,
        price_topic=request.price_topic,
        rate_topic=request.rate_topic,
        product_records_published=len(price_records),
        currency_records_published=len(rate_records),
    )


@app.get("/config", response_model=dict[str, Any])
async def config() -> dict[str, Any]:
    """Expose safe default runtime configuration values."""
    return {
        "default_currency_url": DEFAULT_CURRENCY_URL,
        "default_kafka_servers": DEFAULT_KAFKA_SERVERS,
        "default_price_topic": PRICE_TOPIC,
        "default_rate_topic": FX_TOPIC,
        "default_base_currency": DEFAULT_BASE_CURRENCY,
        "pipeline_endpoint": "/pipeline/run",
    }


if __name__ == "__main__":
    uvicorn.run(
        "sitemap_exchange_rate_processors.backend.connect_scraper_with_kafka:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
    )
