"""PostgreSQL connection and persistence helpers for Delta Nexus."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/delta_nexus_test"
DEFAULT_CURRENCY_SOURCE_URL = "https://www.floatrates.com/"


def mask_database_url(database_url: str) -> str:
    """Return a safe-to-log database URL with the password hidden."""
    parsed = urlsplit(database_url)
    if not parsed.password:
        return database_url

    username = parsed.username or ""
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{username}:***@{host}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


@dataclass(frozen=True)
class DatabaseSettings:
    """Runtime database settings loaded from environment variables."""

    database_url: str = DEFAULT_DATABASE_URL
    schema_name: str = "delta_nexus"
    currency_source_url: str = DEFAULT_CURRENCY_SOURCE_URL

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        return cls(
            database_url=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
            schema_name=os.getenv("DATABASE_SCHEMA", "delta_nexus"),
            currency_source_url=os.getenv(
                "CURRENCY_SOURCE_URL",
                DEFAULT_CURRENCY_SOURCE_URL,
            ),
        )


class PostgresStorage:
    """Small asyncpg-backed repository for the imported PostgreSQL schema."""

    def __init__(self, settings: DatabaseSettings | None = None):
        self.settings = settings or DatabaseSettings.from_env()
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self.pool:
            return

        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.settings.database_url,
                min_size=1,
                max_size=10,
                server_settings={"search_path": f"{self.settings.schema_name},public"},
            )
        except asyncpg.InvalidPasswordError as exc:
            safe_url = mask_database_url(self.settings.database_url)
            raise RuntimeError(
                "PostgreSQL rejected the configured password. "
                f"Check DATABASE_URL. Current value: {safe_url}"
            ) from exc
        except asyncpg.InvalidCatalogNameError as exc:
            safe_url = mask_database_url(self.settings.database_url)
            raise RuntimeError(
                "PostgreSQL database does not exist. "
                f"Create the database or update DATABASE_URL. Current value: {safe_url}"
            ) from exc

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def health_check(self) -> bool:
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            result = await connection.fetchval("SELECT 1")
        return result == 1

    async def count_product_observations_for_topic(self, topic: str) -> int:
        """Count product price observations persisted from one Kafka topic."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM product_price_observations
                WHERE kafka_topic = $1
                """,
                topic,
            )
        return int(count or 0)

    async def count_recent_product_observations_for_topic(
        self,
        topic: str,
        inserted_after: datetime,
    ) -> int:
        """Count product rows for a topic inserted after a timestamp."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM product_price_observations
                WHERE kafka_topic = $1
                  AND created_at >= $2
                """,
                topic,
                inserted_after,
            )
        return int(count or 0)

    async def count_exchange_rate_observations_for_topic(self, topic: str) -> int:
        """Count exchange-rate observations persisted from one Kafka topic."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM exchange_rate_observations
                WHERE kafka_topic = $1
                """,
                topic,
            )
        return int(count or 0)

    async def count_recent_exchange_rate_observations_for_topic(
        self,
        topic: str,
        inserted_after: datetime,
    ) -> int:
        """Count FX rows for a topic inserted after a timestamp."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM exchange_rate_observations
                WHERE kafka_topic = $1
                  AND created_at >= $2
                """,
                topic,
                inserted_after,
            )
        return int(count or 0)

    async def count_processed_messages_for_topics(self, topics: list[str]) -> int:
        """Count Kafka processed-message ledger rows for a topic list."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM kafka_processed_messages
                WHERE topic = ANY($1::text[])
                """,
                topics,
            )
        return int(count or 0)

    async def count_recent_processed_messages_for_topics(
        self,
        topics: list[str],
        processed_after: datetime,
    ) -> int:
        """Count processed-message ledger rows inserted after a timestamp."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            count = await connection.fetchval(
                """
                SELECT count(*)
                FROM kafka_processed_messages
                WHERE topic = ANY($1::text[])
                  AND processed_at >= $2
                """,
                topics,
                processed_after,
            )
        return int(count or 0)

    async def save_product_price_event(
        self,
        event: Any,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        """Persist one validated ProductPriceEvent."""
        if not self.pool:
            await self.connect()

        source_url = event.source_url or "unknown-product-feed"
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    SELECT upsert_product_price_observation(
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                    )
                    """,
                    source_url,
                    event.product_id,
                    event.product_name,
                    event.product_url,
                    event.price,
                    event.currency.upper(),
                    event.timestamp,
                    event.emitted_at,
                    event.schema_version,
                    topic,
                    partition,
                    offset,
                )

    async def save_currency_rate_event(
        self,
        event: Any,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        """Persist one validated CurrencyRateEvent."""
        if not self.pool:
            await self.connect()

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    SELECT upsert_exchange_rate_observation(
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                    )
                    """,
                    self.settings.currency_source_url,
                    event.base_currency.upper(),
                    event.target_currency.upper(),
                    event.rate,
                    event.timestamp,
                    event.emitted_at,
                    event.schema_version,
                    topic,
                    partition,
                    offset,
                )

    async def record_processed_message(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        consumer_group_id: str,
        schema_version: str,
        event_type: str,
        event_key: bytes | None,
        event_payload: bytes,
    ) -> None:
        """Record a Kafka message as processed for idempotency/audit."""
        if not self.pool:
            await self.connect()

        event_hash = sha256(event_payload).digest()
        event_key_text = event_key.decode("utf-8") if event_key else None

        assert self.pool is not None
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO kafka_processed_messages (
                    topic,
                    partition,
                    offset_value,
                    consumer_group_id,
                    schema_version,
                    event_type,
                    event_key,
                    event_hash,
                    processed_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT DO NOTHING
                """,
                topic,
                partition,
                offset,
                consumer_group_id,
                schema_version,
                event_type,
                event_key_text,
                event_hash,
                datetime.now().astimezone(),
            )
