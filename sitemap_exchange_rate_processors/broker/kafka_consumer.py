import asyncio
import csv
import json
from pathlib import Path
from aiokafka import AIOKafkaConsumer
from loguru import logger
from .broker import ProductPriceRecord, CurrencyRateRecord


class KafkaConsumerWriter:
    """
    Consumes records from Kafka topics and writes them to CSV files.
    This is an alternative to writing during scraping—records flow through Kafka first.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        price_topic: str = "product-prices",
        rate_topic: str = "currency-rates",
        output_dir: str | Path = ".",
        group_id: str = "delta-nexus-consumer",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.price_topic = price_topic
        self.rate_topic = rate_topic
        self.output_dir = Path(output_dir)
        self.group_id = group_id
        self.price_records: list[dict] = []
        self.rate_records: list[dict] = []

    async def start_consuming(self, timeout_seconds: int = 60) -> None:
        """
        Start consuming from both topics and writing to CSV.
        Runs for `timeout_seconds` (how long to wait for new messages before stopping).
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        consumer = AIOKafkaConsumer(
            self.price_topic,
            self.rate_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        await consumer.start()
        logger.info(
            f"[KAFKA_CONSUMER] Started consuming from '{self.price_topic}' and '{self.rate_topic}'"
        )

        try:
            # Consume with timeout to exit gracefully
            async with asyncio.timeout(timeout_seconds):
                async for message in consumer:
                    if message.topic == self.price_topic:
                        try:
                            record = ProductPriceRecord(**message.value)
                            self.price_records.append(record.model_dump(mode="json"))
                            logger.debug(
                                f"[KAFKA_CONSUMER] Received price: {record.product_id}"
                            )
                        except Exception as e:
                            logger.error(f"[KAFKA_CONSUMER] Invalid price record: {e}")

                    elif message.topic == self.rate_topic:
                        try:
                            record = CurrencyRateRecord(**message.value)
                            self.rate_records.append(record.model_dump(mode="json"))
                            logger.debug(
                                f"[KAFKA_CONSUMER] Received rate: "
                                f"{record.base_currency}/{record.target_currency}"
                            )
                        except Exception as e:
                            logger.error(f"[KAFKA_CONSUMER] Invalid rate record: {e}")

        except asyncio.TimeoutError:
            logger.info(f"[KAFKA_CONSUMER] Timeout reached. Writing to CSV...")
        finally:
            await consumer.stop()
            self._write_csv_files()

    def _write_csv_files(self) -> None:
        """Write collected records to CSV files."""
        if self.price_records:
            price_path = self.output_dir / "scraped_product_prices.csv"
            with price_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "product_id",
                        "product_name",
                        "product_url",
                        "source_url",
                        "price",
                        "currency",
                        "timestamp",
                    ],
                )
                writer.writeheader()
                writer.writerows(self.price_records)
            logger.info(f"[KAFKA_CONSUMER] Wrote {len(self.price_records)} prices to {price_path}")

        if self.rate_records:
            rate_path = self.output_dir / "processed_currency_rates.csv"
            with rate_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["base_currency", "target_currency", "rate", "timestamp"],
                )
                writer.writeheader()
                writer.writerows(self.rate_records)
            logger.info(f"[KAFKA_CONSUMER] Wrote {len(self.rate_records)} rates to {rate_path}")


async def consume_from_kafka(
    bootstrap_servers: str = "localhost:9092",
    price_topic: str = "product-prices",
    rate_topic: str = "currency-rates",
    output_dir: str | Path = ".",
    timeout_seconds: int = 60,
) -> None:
    """Convenience function to start consuming from Kafka."""
    consumer_writer = KafkaConsumerWriter(
        bootstrap_servers=bootstrap_servers,
        price_topic=price_topic,
        rate_topic=rate_topic,
        output_dir=output_dir,
    )
    await consumer_writer.start_consuming(timeout_seconds=timeout_seconds)


if __name__ == "__main__":
    asyncio.run(
        consume_from_kafka(
            bootstrap_servers="localhost:9092",
            output_dir="outputs",
            timeout_seconds=30,
        )
    )
