"""Kafka consumer that persists Delta Nexus events into PostgreSQL."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from loguru import logger

from sitemap_exchange_rate_processors.broker import (
    FX_TOPIC,
    PRICE_TOPIC,
    SCHEMA_VERSION,
    CurrencyRateEvent,
    ProductPriceEvent,
)
from sitemap_exchange_rate_processors.storage import DatabaseSettings, PostgresStorage


class KafkaPostgresConsumer:
    """Consume validated Kafka events and persist them to PostgreSQL."""

    def __init__(
        self,
        *,
        bootstrap_servers: str = "localhost:9092",
        price_topic: str = PRICE_TOPIC,
        rate_topic: str = FX_TOPIC,
        group_id: str = "delta-nexus-postgres-consumer",
        database_settings: DatabaseSettings | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.price_topic = price_topic
        self.rate_topic = rate_topic
        self.group_id = group_id
        self.storage = PostgresStorage(database_settings)
        self.product_events_saved = 0
        self.currency_events_saved = 0

    async def start_consuming(self, timeout_seconds: int = 60) -> None:
        """Consume Kafka events and write them to PostgreSQL."""
        await self.storage.connect()

        consumer = AIOKafkaConsumer(
            self.price_topic,
            self.rate_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        await consumer.start()
        logger.info(
            "[KAFKA_POSTGRES] Consuming '{}' and '{}' into PostgreSQL",
            self.price_topic,
            self.rate_topic,
        )

        try:
            async with asyncio.timeout(timeout_seconds):
                async for message in consumer:
                    await self._handle_message(message)
                    await consumer.commit()
        except asyncio.TimeoutError:
            logger.info("[KAFKA_POSTGRES] Timeout reached. Stopping consumer.")
        finally:
            await consumer.stop()
            await self.storage.disconnect()

    async def _handle_message(self, message) -> None:
        raw_payload = bytes(message.value)
        payload = json.loads(raw_payload.decode("utf-8"))

        if message.topic == self.price_topic:
            event = ProductPriceEvent(**payload)
            if event.schema_version != SCHEMA_VERSION:
                raise ValueError(f"Unsupported schema_version {event.schema_version}")

            await self.storage.save_product_price_event(
                event,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            self.product_events_saved += 1
            if self.product_events_saved <= 5 or self.product_events_saved % 100 == 0:
                logger.info(
                    "[KAFKA_POSTGRES] Saved product event #{}: "
                    "topic={} partition={} offset={} id={} price={} {}",
                    self.product_events_saved,
                    message.topic,
                    message.partition,
                    message.offset,
                    event.product_id,
                    event.price,
                    event.currency,
                )

        elif message.topic == self.rate_topic:
            event = CurrencyRateEvent(**payload)
            if event.schema_version != SCHEMA_VERSION:
                raise ValueError(f"Unsupported schema_version {event.schema_version}")

            await self.storage.save_currency_rate_event(
                event,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            self.currency_events_saved += 1
            if self.currency_events_saved <= 5 or self.currency_events_saved % 25 == 0:
                logger.info(
                    "[KAFKA_POSTGRES] Saved FX event #{}: "
                    "topic={} partition={} offset={} pair={}/{} rate={}",
                    self.currency_events_saved,
                    message.topic,
                    message.partition,
                    message.offset,
                    event.base_currency,
                    event.target_currency,
                    event.rate,
                )

        else:
            raise ValueError(f"Unexpected topic {message.topic}")

        await self.storage.record_processed_message(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            consumer_group_id=self.group_id,
            schema_version=payload.get("schema_version", ""),
            event_type=payload.get("event_type", ""),
            event_key=message.key,
            event_payload=raw_payload,
        )


async def consume_kafka_to_postgres(
    *,
    bootstrap_servers: str = "localhost:9092",
    price_topic: str = PRICE_TOPIC,
    rate_topic: str = FX_TOPIC,
    group_id: str = "delta-nexus-postgres-consumer",
    database_url: str | None = None,
    timeout_seconds: int = 60,
) -> None:
    """Convenience entry point for the PostgreSQL Kafka consumer."""
    settings = DatabaseSettings.from_env()
    if database_url:
        settings = DatabaseSettings(
            database_url=database_url,
            schema_name=settings.schema_name,
            currency_source_url=settings.currency_source_url,
        )

    consumer = KafkaPostgresConsumer(
        bootstrap_servers=bootstrap_servers,
        price_topic=price_topic,
        rate_topic=rate_topic,
        group_id=group_id,
        database_settings=settings,
    )
    await consumer.start_consuming(timeout_seconds=timeout_seconds)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Consume Delta Nexus Kafka events into PostgreSQL."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--price-topic", default=PRICE_TOPIC)
    parser.add_argument("--rate-topic", default=FX_TOPIC)
    parser.add_argument("--group-id", default="delta-nexus-postgres-consumer")
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    args = parser.parse_args()

    asyncio.run(
        consume_kafka_to_postgres(
            bootstrap_servers=args.bootstrap_servers,
            price_topic=args.price_topic,
            rate_topic=args.rate_topic,
            group_id=args.group_id,
            database_url=args.database_url,
            timeout_seconds=args.timeout_seconds,
        )
    )
