import json
from contextlib import suppress
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from loguru import logger

from .broker import (
    FX_TOPIC,
    PRICE_TOPIC,
    SCHEMA_VERSION,
    CurrencyRateRecord,
    MessageBrokerClient,
    ProductPriceRecord,
)


class KafkaProducerClient(MessageBrokerClient):
    """Publish validated product and currency events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        price_topic: str = PRICE_TOPIC,
        rate_topic: str = FX_TOPIC,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.price_topic = price_topic
        self.rate_topic = rate_topic
        self.producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        """Start the Kafka producer or raise when Kafka is unavailable."""
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(
                value,
                default=str,
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        try:
            await producer.start()
        except Exception:
            with suppress(Exception):
                await producer.stop()
            raise

        self.producer = producer
        logger.info(f"[KAFKA] Connected producer to {self.bootstrap_servers}")

    async def disconnect(self) -> None:
        """Close the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None
            logger.info("[KAFKA] Producer disconnected")

    async def send_price_data(self, record: ProductPriceRecord) -> None:
        """Publish one product-price event."""
        if not self.producer:
            raise RuntimeError("Kafka producer is not connected.")

        event = {
            "schema_version": SCHEMA_VERSION,
            "event_type": "product_price",
            "emitted_at": datetime.now(timezone.utc),
            **record.model_dump(mode="json"),
        }
        try:
            await self.producer.send_and_wait(
                self.price_topic,
                value=event,
                key=record.product_id.encode("utf-8"),
            )
        except Exception:
            logger.exception("[KAFKA] Failed to publish product-price event")
            raise

        logger.debug(
            f"[KAFKA] Sent price to '{self.price_topic}': "
            f"{record.product_id} @ {record.price} {record.currency}"
        )

    async def send_currency_data(self, record: CurrencyRateRecord) -> None:
        """Publish one currency-rate event."""
        if not self.producer:
            raise RuntimeError("Kafka producer is not connected.")

        key = f"{record.base_currency}/{record.target_currency}".encode("utf-8")
        event = {
            "schema_version": SCHEMA_VERSION,
            "event_type": "currency_rate",
            "emitted_at": datetime.now(timezone.utc),
            **record.model_dump(mode="json"),
        }
        try:
            await self.producer.send_and_wait(
                self.rate_topic,
                value=event,
                key=key,
            )
        except Exception:
            logger.exception("[KAFKA] Failed to publish currency-rate event")
            raise

        logger.debug(
            f"[KAFKA] Sent rate to '{self.rate_topic}': "
            f"{record.base_currency}/{record.target_currency} = {record.rate}"
        )
