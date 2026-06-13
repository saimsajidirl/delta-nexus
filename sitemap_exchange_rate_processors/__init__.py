"""Delta Nexus: A Python scraping pipeline with Kafka integration."""

from .broker import (
    FX_TOPIC,
    PRICE_TOPIC,
    CurrencyRateEvent,
    MessageBrokerClient,
    EventEnvelope,
    ProductPriceEvent,
    ProductPriceRecord,
    CurrencyRateRecord,
    KafkaMessageBrokerClient,
    KafkaConsumerWriter,
    consume_from_kafka,
    SCHEMA_VERSION,
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
    "EventEnvelope",
    "ProductPriceEvent",
    "ProductPriceRecord",
    "CurrencyRateRecord",
    "CurrencyRateEvent",
    "SCHEMA_VERSION",
    "PRICE_TOPIC",
    "FX_TOPIC",
    "KafkaMessageBrokerClient",
    "KafkaConsumerWriter",
    "consume_from_kafka",
    "run_delta_nexus_engine",
]
