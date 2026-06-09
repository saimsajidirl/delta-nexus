"""Base broker interface for Delta Nexus."""

from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from datetime import datetime, timezone


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
