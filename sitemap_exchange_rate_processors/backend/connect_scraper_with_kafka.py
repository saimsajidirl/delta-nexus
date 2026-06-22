"""FastAPI app that triggers the scraper-to-Kafka Delta Nexus pipeline."""

import asyncio
import os
import re
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
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
from sitemap_exchange_rate_processors.consumers import KafkaPostgresConsumer
from sitemap_exchange_rate_processors.storage import DatabaseSettings, PostgresStorage


DEFAULT_CURRENCY_URL = os.getenv("CURRENCY_URL", ENGINE_DEFAULT_CURRENCY_URL)
DEFAULT_KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{5,63}$")
SESSION_LOG_LIMIT = 200
SESSION_LOGS: dict[str, list[dict[str, str]]] = {}

app = FastAPI(
    title="Delta Nexus Scraper Kafka API",
    version="1.0.0",
    description=(
        "API layer that triggers product and currency scraping, then publishes "
        "validated events to Kafka."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
    session_id: str | None = None
    price_url: str
    currency_url: str
    kafka_servers: str
    price_topic: str
    rate_topic: str
    product_records_published: int
    currency_records_published: int
    sample_products: list[dict[str, Any]]
    sample_rates: list[dict[str, Any]]


class SessionPipelineRunRequest(BaseModel):
    """Request body for one session-scoped pipeline run."""

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
    base_currency: str = Field(
        default=DEFAULT_BASE_CURRENCY,
        min_length=3,
        max_length=3,
        description="Base currency used for the exchange-rate snapshot.",
    )
    consumer_timeout_seconds: int = Field(
        default=60,
        ge=5,
        le=300,
        description="Maximum time the session Kafka-to-Postgres consumer stays open.",
    )
    persist_wait_seconds: int = Field(
        default=20,
        ge=0,
        le=120,
        description="Maximum time to wait for published events to appear in Postgres.",
    )


class SessionDataResponse(BaseModel):
    """Session-scoped data returned to the React dashboard."""

    session_id: str
    price_topic: str
    rate_topic: str
    database_status: str = "ok"
    database_error: str | None = None
    products_count: int
    exchange_rates_count: int
    products: list[dict[str, Any]]
    exchange_rates: list[dict[str, Any]]
    logs: list[dict[str, str]]


class HealthResponse(BaseModel):
    """Basic service health response."""

    status: str
    service: str


class ReadinessResponse(BaseModel):
    """Dependency readiness response."""

    status: str
    postgres: str
    database_url_configured: bool


def get_session_topics(session_id: str) -> tuple[str, str]:
    """Return deterministic Kafka topics for a frontend session."""
    if not SESSION_ID_PATTERN.fullmatch(session_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "session_id must be 6-64 characters and only contain letters, "
                "numbers, underscores, or hyphens."
            ),
        )
    return f"{PRICE_TOPIC}-{session_id}", f"{FX_TOPIC}-{session_id}"


def add_session_log(session_id: str, level: str, message: str) -> None:
    """Append one in-memory log entry for the current dashboard session."""
    entries = SESSION_LOGS.setdefault(session_id, [])
    entries.append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
        }
    )
    if len(entries) > SESSION_LOG_LIMIT:
        del entries[: len(entries) - SESSION_LOG_LIMIT]


@app.get("/", response_model=dict[str, str])
async def root() -> dict[str, str]:
    """Return a tiny service index."""
    return {
        "service": "Delta Nexus Scraper Kafka API",
        "docs": "/docs",
        "health": "/health",
        "run_pipeline": "/pipeline/run",
        "run_session_pipeline": "/sessions/{session_id}/pipeline/run",
        "session_data": "/sessions/{session_id}/data",
    }


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Confirm that the FastAPI process is running."""
    return HealthResponse(
        status="ok",
        service="delta-nexus-scraper-kafka-api",
    )


@app.get("/ready", response_model=ReadinessResponse)
async def ready() -> ReadinessResponse:
    """Confirm that FastAPI can reach PostgreSQL."""
    settings = DatabaseSettings.from_env()
    storage = PostgresStorage(settings)
    try:
        await storage.connect()
        await storage.health_check()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "PostgreSQL is not reachable.",
                "error": str(exc),
            },
        ) from exc
    finally:
        await storage.disconnect()

    return ReadinessResponse(
        status="ready",
        postgres="ok",
        database_url_configured=bool(settings.database_url),
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
        logger.info(
            "[API] Received pipeline run request: price_url={} currency_url={} "
            "base_currency={} price_topic={} rate_topic={} kafka={}",
            request.price_url,
            request.currency_url,
            request.base_currency.upper(),
            request.price_topic,
            request.rate_topic,
            request.kafka_servers,
        )
        logger.info("[API] Connecting Kafka producer")
        await producer.connect()
        logger.info("[API] Starting product and FX scraping tasks")
        price_records, rate_records = await asyncio.gather(
            run_price_task(AsyncPriceScraper(), str(request.price_url), producer),
            run_currency_task(
                AsyncCurrencyFetcher(str(request.currency_url)),
                request.base_currency.upper(),
                producer,
            ),
        )
        logger.info(
            "[API] Pipeline run complete: published {} product events and {} FX events",
            len(price_records),
            len(rate_records),
        )
    except Exception as exc:
        logger.exception("[API] Pipeline run failed")
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
        sample_products=[
            record.model_dump(mode="json") for record in price_records[:5]
        ],
        sample_rates=[
            record.model_dump(mode="json") for record in rate_records[:5]
        ],
    )


@app.post(
    "/sessions/{session_id}/pipeline/run",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_200_OK,
)
async def run_session_pipeline(
    session_id: str,
    request: SessionPipelineRunRequest,
) -> PipelineRunResponse:
    """
    Run one session-scoped scrape job.

    The session id is mapped to unique Kafka topics. A short-lived PostgreSQL
    consumer runs while the scraper publishes so the React dashboard can poll
    persisted rows by session.
    """
    price_topic, rate_topic = get_session_topics(session_id)
    group_id = f"delta-nexus-postgres-consumer-{session_id}"
    SESSION_LOGS[session_id] = []
    add_session_log(session_id, "info", "Session pipeline requested")
    add_session_log(session_id, "info", f"Product topic: {price_topic}")
    add_session_log(session_id, "info", f"FX topic: {rate_topic}")
    producer = KafkaProducerClient(
        bootstrap_servers=request.kafka_servers,
        price_topic=price_topic,
        rate_topic=rate_topic,
    )
    postgres_consumer = KafkaPostgresConsumer(
        bootstrap_servers=request.kafka_servers,
        price_topic=price_topic,
        rate_topic=rate_topic,
        group_id=group_id,
    )
    consumer_task = asyncio.create_task(
        postgres_consumer.start_consuming(
            timeout_seconds=request.consumer_timeout_seconds,
        )
    )

    try:
        add_session_log(session_id, "info", "Starting Kafka to Postgres consumer")
        await asyncio.sleep(1)
        logger.info(
            "[API] Received session pipeline run: session={} price_topic={} "
            "rate_topic={} kafka={}",
            session_id,
            price_topic,
            rate_topic,
            request.kafka_servers,
        )
        add_session_log(session_id, "info", "Connecting Kafka producer")
        await producer.connect()
        add_session_log(session_id, "info", "Scraping product feed and FX rates")
        price_records, rate_records = await asyncio.gather(
            run_price_task(AsyncPriceScraper(), str(request.price_url), producer),
            run_currency_task(
                AsyncCurrencyFetcher(str(request.currency_url)),
                request.base_currency.upper(),
                producer,
            ),
        )
        add_session_log(
            session_id,
            "success",
            f"Published {len(price_records)} product events and {len(rate_records)} FX events",
        )

        storage = PostgresStorage()
        add_session_log(session_id, "info", "Waiting for persisted Postgres rows")
        await storage.connect()
        try:
            deadline = asyncio.get_running_loop().time() + request.persist_wait_seconds
            while asyncio.get_running_loop().time() < deadline:
                products_saved = await storage.count_product_observations_for_topic(
                    price_topic,
                )
                rates_saved = await storage.count_exchange_rate_observations_for_topic(
                    rate_topic,
                )
                add_session_log(
                    session_id,
                    "info",
                    f"Persisted rows: {products_saved}/{len(price_records)} products, "
                    f"{rates_saved}/{len(rate_records)} FX rates",
                )
                if products_saved >= len(price_records) and rates_saved >= len(rate_records):
                    break
                await asyncio.sleep(1)
        finally:
            await storage.disconnect()
        add_session_log(session_id, "success", "Session pipeline completed")

    except Exception as exc:
        logger.exception("[API] Session pipeline run failed")
        add_session_log(session_id, "error", f"Pipeline failed: {exc}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Session pipeline failed while scraping, publishing, or persisting.",
                "error": str(exc),
            },
        ) from exc
    finally:
        await producer.disconnect()
        if not consumer_task.done():
            consumer_task.cancel()
        with suppress(asyncio.CancelledError):
            try:
                await consumer_task
            except Exception as exc:
                add_session_log(session_id, "error", f"Background consumer failed: {exc}")
        add_session_log(session_id, "info", "Background consumer stopped")

    return PipelineRunResponse(
        status="completed",
        message="Session data was scraped, published to Kafka, and persisted to PostgreSQL.",
        session_id=session_id,
        price_url=str(request.price_url),
        currency_url=str(request.currency_url),
        kafka_servers=request.kafka_servers,
        price_topic=price_topic,
        rate_topic=rate_topic,
        product_records_published=len(price_records),
        currency_records_published=len(rate_records),
        sample_products=[
            record.model_dump(mode="json") for record in price_records[:5]
        ],
        sample_rates=[
            record.model_dump(mode="json") for record in rate_records[:5]
        ],
    )


@app.get("/sessions/{session_id}/data", response_model=SessionDataResponse)
async def get_session_data(session_id: str, limit: int = 100) -> SessionDataResponse:
    """Return product and exchange-rate rows stored for one frontend session."""
    price_topic, rate_topic = get_session_topics(session_id)
    storage = PostgresStorage()
    database_status = "ok"
    database_error = None
    products_count = 0
    exchange_rates_count = 0
    products: list[dict[str, Any]] = []
    exchange_rates: list[dict[str, Any]] = []
    try:
        await storage.connect()
        products_count, exchange_rates_count, products, exchange_rates = await asyncio.gather(
            storage.count_product_observations_for_topic(price_topic),
            storage.count_exchange_rate_observations_for_topic(rate_topic),
            storage.list_product_observations_for_topic(price_topic, limit=limit),
            storage.list_exchange_rate_observations_for_topic(rate_topic, limit=limit),
        )
    except Exception as exc:
        database_status = "error"
        database_error = str(exc)
        if SESSION_LOGS.get(session_id):
            add_session_log(session_id, "error", f"PostgreSQL read failed: {exc}")
    finally:
        await storage.disconnect()

    return SessionDataResponse(
        session_id=session_id,
        price_topic=price_topic,
        rate_topic=rate_topic,
        database_status=database_status,
        database_error=database_error,
        products_count=products_count,
        exchange_rates_count=exchange_rates_count,
        products=products,
        exchange_rates=exchange_rates,
        logs=SESSION_LOGS.get(session_id, []),
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
        "database_url_configured": bool(DatabaseSettings.from_env().database_url),
        "pipeline_endpoint": "/pipeline/run",
        "session_pipeline_endpoint": "/sessions/{session_id}/pipeline/run",
        "session_data_endpoint": "/sessions/{session_id}/data",
    }


if __name__ == "__main__":
    uvicorn.run(
        "sitemap_exchange_rate_processors.backend.connect_scraper_with_kafka:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
    )
