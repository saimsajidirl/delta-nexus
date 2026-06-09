import json
from aiokafka import AIOKafkaProducer
from loguru import logger
from .broker import (
    MessageBrokerClient,
    ProductPriceRecord,
    CurrencyRateRecord,
)


class KafkaMessageBrokerClient(MessageBrokerClient):
    """
    Kafka-based implementation of the message broker.
    Sends price and rate records to separate Kafka topics.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        price_topic: str = "product-prices",
        rate_topic: str = "currency-rates",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.price_topic = price_topic
        self.rate_topic = rate_topic
        self.producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        """Initialize the Kafka producer."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        await self.producer.start()
        logger.info(f"[KAFKA] Connected to {self.bootstrap_servers}")

    async def disconnect(self) -> None:
        """Close the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("[KAFKA] Disconnected")

    async def send_price_data(self, record: ProductPriceRecord) -> None:
        """Send product price record to Kafka."""
        if not self.producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")

        try:
            await self.producer.send_and_wait(
                self.price_topic,
                value=record.model_dump(mode="json"),
                key=record.product_id.encode("utf-8"),
            )
            logger.debug(
                f"[KAFKA] Sent price to '{self.price_topic}': "
                f"{record.product_id} @ {record.price} {record.currency}"
            )
        except Exception as e:
            logger.error(f"[KAFKA] Failed to send price data: {e}")
            raise

    async def send_currency_data(self, record: CurrencyRateRecord) -> None:
        """Send currency rate record to Kafka."""
        if not self.producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")

        try:
            key = f"{record.base_currency}/{record.target_currency}".encode("utf-8")
            await self.producer.send_and_wait(
                self.rate_topic,
                value=record.model_dump(mode="json"),
                key=key,
            )
            logger.debug(
                f"[KAFKA] Sent rate to '{self.rate_topic}': "
                f"{record.base_currency}/{record.target_currency} = {record.rate}"
            )
        except Exception as e:
            logger.error(f"[KAFKA] Failed to send rate data: {e}")
            raise
