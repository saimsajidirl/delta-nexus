"""Delta Nexus: A Python scraping pipeline with Kafka integration."""

from .broker import (
    MessageBrokerClient,
    ProductPriceRecord,
    CurrencyRateRecord,
    KafkaMessageBrokerClient,
    KafkaConsumerWriter,
    consume_from_kafka,
)
from .scrapers import (
    AsyncPriceScraper,
    AsyncCurrencyFetcher,
    run_delta_nexus_engine,
)

__all__ = [
    "AsyncPriceScraper",
    "AsyncCurrencyFetcher",
    "MessageBrokerClient",
    "ProductPriceRecord",
    "CurrencyRateRecord",
    "KafkaMessageBrokerClient",
    "KafkaConsumerWriter",
    "consume_from_kafka",
    "run_delta_nexus_engine",
]
