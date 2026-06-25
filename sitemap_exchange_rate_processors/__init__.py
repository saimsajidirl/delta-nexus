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
    KafkaConsumerWriter,
    KafkaProducerClient,
    consume_from_kafka,
    SCHEMA_VERSION,
)
from .scrapers import (
    run_aliexpress_scraper,
    AsyncCurrencyFetcher,
    run_delta_nexus_engine,
)

__all__ = [
    "run_aliexpress_scraper",
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
    "KafkaProducerClient",
    "KafkaConsumerWriter",
    "consume_from_kafka",
    "run_delta_nexus_engine",
]
