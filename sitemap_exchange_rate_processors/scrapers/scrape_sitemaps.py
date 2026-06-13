"""Product feed scraping for Delta Nexus."""

import argparse
import asyncio
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import AsyncIterator, Iterator

import httpx
from loguru import logger
from lxml import etree
from pydantic import ValidationError

from ..broker import ProductPriceRecord


DEFAULT_USER_AGENT = "DeltaNexus/1.0 (Price Scraper)"
PRODUCT_TAGS = {"item", "product", "url"}


class ScraperError(Exception):
    """Base exception for product scraper failures."""


class ScraperIOError(ScraperError):
    """Raised when a product feed cannot be read."""


class ScraperParsingError(ScraperError):
    """Raised when a product feed contains malformed XML."""


def _local_name(tag: object) -> str:
    """Return an XML tag name without its namespace."""
    text = str(tag)
    if "}" in text:
        return text.rsplit("}", 1)[1]
    return text.split(":", 1)[-1]


def _first_text(element: etree._Element, *names: str) -> str | None:
    wanted = set(names)
    for child in element.iterchildren():
        if _local_name(child.tag) in wanted and child.text:
            return child.text.strip()
    return None


def parse_product_element(
    element: etree._Element,
    source_url: str | None = None,
) -> ProductPriceRecord | None:
    """Convert one XML product element into the shared product schema."""
    try:
        product_id = _first_text(element, "id", "sku", "item_id", "product_id")
        product_name = _first_text(element, "title", "name", "product_name")
        product_url = _first_text(element, "link", "url", "loc", "product_url")
        raw_price = _first_text(element, "price")
        raw_timestamp = _first_text(
            element,
            "timestamp",
            "updated_at",
            "last_updated",
            "pubDate",
        )
        currency = _first_text(element, "currency") or "USD"

        if not product_id or not raw_price:
            return None

        price_parts = raw_price.split()
        if len(price_parts) > 1 and currency == "USD":
            currency = price_parts[-1]

        timestamp = (
            datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
            if raw_timestamp
            else datetime.now(timezone.utc)
        )
        return ProductPriceRecord(
            product_id=product_id,
            product_name=product_name,
            product_url=product_url,
            source_url=source_url,
            price=float(price_parts[0]),
            currency=currency,
            timestamp=timestamp,
        )
    except (TypeError, ValueError, ValidationError) as exc:
        logger.warning(f"Skipping invalid product element '{element.tag}': {exc}")
        return None


def _iter_product_elements(source: str | Path) -> Iterator[etree._Element]:
    try:
        context = etree.iterparse(str(source), events=("end",), recover=False)
        for _, element in context:
            if _local_name(element.tag) not in PRODUCT_TAGS:
                continue
            yield element
            element.clear()
            while element.getprevious() is not None:
                del element.getparent()[0]
    except etree.XMLSyntaxError as exc:
        raise ScraperParsingError(f"Failed to parse XML: {exc}") from exc
    except (OSError, IOError) as exc:
        raise ScraperIOError(f"Failed to read product feed: {exc}") from exc


class AsyncPriceScraper:
    """Fetch and incrementally parse XML product feeds."""

    def __init__(self, user_agent: str = DEFAULT_USER_AGENT):
        self.headers = {"User-Agent": user_agent}

    async def stream_prices(
        self,
        source_url: str,
    ) -> AsyncIterator[ProductPriceRecord]:
        logger.info(f"[PRICE_SCRAPER] Starting stream from {source_url}")

        async with httpx.AsyncClient(headers=self.headers, timeout=60.0) as client:
            response = await client.get(source_url)
            response.raise_for_status()

        try:
            context = etree.iterparse(
                BytesIO(response.content),
                events=("end",),
                recover=False,
            )
            for _, element in context:
                if _local_name(element.tag) not in PRODUCT_TAGS:
                    continue

                record = parse_product_element(element, source_url)
                await asyncio.sleep(0)
                if record:
                    yield record

                element.clear()
                while element.getprevious() is not None:
                    del element.getparent()[0]
        except etree.XMLSyntaxError as exc:
            raise ScraperParsingError(f"Failed to parse XML: {exc}") from exc


def run_sitemaps_scraper(
    source_path: str | Path,
) -> Iterator[ProductPriceRecord]:
    """Backward-compatible synchronous entry point for local XML files."""
    logger.info(f"Starting product scraper on source: {source_path}")
    for element in _iter_product_elements(source_path):
        record = parse_product_element(element, str(source_path))
        if record:
            yield record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Nexus product feed scraper")
    parser.add_argument("--source", required=True, help="Path to an XML product feed")
    args = parser.parse_args()

    try:
        for product_record in run_sitemaps_scraper(args.source):
            logger.info(product_record.model_dump_json())
    except ScraperError as exc:
        logger.critical(f"Scraper failed: {exc}")
        raise SystemExit(1) from exc
