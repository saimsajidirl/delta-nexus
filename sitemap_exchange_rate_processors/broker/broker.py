"""Base broker interface for Delta Nexus."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pydantic import BaseModel, Field


SCHEMA_VERSION = "1.0"
PRICE_TOPIC = "raw-prices"
FX_TOPIC = "fx-rates"


class EventEnvelope(BaseModel):
    """Versioned event wrapper shared by all Kafka messages."""

    schema_version: str = Field(default=SCHEMA_VERSION, description="Message schema version.")
    event_type: str = Field(..., min_length=1, description="Type of event payload.")
    emitted_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when the event was emitted.",
    )


class ProductPriceEvent(EventEnvelope):
    """Kafka event for scraped product prices."""

    event_type: str = Field(default="product_price", frozen=True)
    product_id: str = Field(..., min_length=1)
    product_name: str | None = None
    product_url: str | None = None
    source_url: str | None = None
    price: float = Field(..., gt=0)
    currency: str = Field(default="USD")
    timestamp: datetime


class CurrencyRateEvent(EventEnvelope):
    """Kafka event for scraped FX rates."""

    event_type: str = Field(default="currency_rate", frozen=True)
    base_currency: str
    target_currency: str
    rate: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProductPriceRecord(BaseModel):
    """Schema for scraped product data."""
    product_id: str = Field(..., min_length=1)
    product_name: str | None = None
    product_url: str | None = None
    source_url: str | None = None
    price: float = Field(..., gt=0)
    currency: str = Field(default="USD")
    timestamp: datetime


class CurrencyRateRecord(BaseModel):
    """Schema for scraped exchange rates."""
    base_currency: str
    target_currency: str
    rate: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MessageBrokerClient(ABC):
    """Abstract base class for message broker implementations."""

    @abstractmethod
    async def send_price_data(self, record: ProductPriceRecord) -> None:
        """Send product price record to the broker."""
        pass

    @abstractmethod
    async def send_currency_data(self, record: CurrencyRateRecord) -> None:
        """Send currency rate record to the broker."""
        pass
