import asyncio
import csv
import re
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Union

import httpx
from lxml import etree
from loguru import logger
from pydantic import BaseModel, Field, ValidationError
from selectolax.parser import HTMLParser

# --- CONSTANTS ---
DEFAULT_USER_AGENT: str = "DeltaNexus/1.0 (Unified Engine)"
DEFAULT_CURRENCY_URL: str = "https://www.floatrates.com/daily.html"
DEFAULT_BASE_CURRENCY: str = "USD"
CURRENCY_FETCH_INTERVAL: float = 30.0

# --- DATA MODELS ---

class ProductPriceRecord(BaseModel):
    """Schema for scraped product data."""
    product_id: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    currency: str = Field(default="USD")
    timestamp: datetime

class CurrencyRateRecord(BaseModel):
    """Schema for scraped exchange rates."""
    base_currency: str
    target_currency: str
    rate: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# --- BROKER INTERFACE (Pre-Task 2) ---

class MessageBrokerClient:
    """
    Acts as the gateway to Task 2. 
    Currently logs hand-offs; will be replaced by Kafka/RabbitMQ producer.
    """
    async def send_price_data(self, record: ProductPriceRecord) -> None:
        # In Task 2, this becomes: await self.producer.send("prices", record.json())
        logger.debug(f"[BROKER] Handoff Price: {record.product_id} @ {record.price} {record.currency}")

    async def send_currency_data(self, record: CurrencyRateRecord) -> None:
        # In Task 2, this becomes: await self.producer.send("rates", record.json())
        logger.debug(f"[BROKER] Handoff Rate: {record.base_currency}/{record.target_currency} = {record.rate}")

# --- COLLECTOR 1: THE PRICE SCRAPER (ASYNC) ---

class AsyncPriceScraper:
    """
    High-efficiency XML sitemap scraper.
    """
    def __init__(self, user_agent: str = DEFAULT_USER_AGENT):
        self.headers = {"User-Agent": user_agent}

    async def stream_prices(self, source_url: str) -> AsyncIterator[ProductPriceRecord]:
        logger.info(f"[PRICE_SCRAPER] Starting stream from {source_url}")
        
        async with httpx.AsyncClient(headers=self.headers, timeout=60.0) as client:
            try:
                response = await client.get(source_url)
                response.raise_for_status()
                
                # Convert bytes to a file-like object for lxml.iterparse
                # In a real production environment with massive files, 
                # we would stream the response body directly using response.aiter_bytes()
                xml_data = response.content
                
                # We use iterparse for memory efficiency
                context = etree.iterparse(
                    BytesIO(xml_data), 
                    events=("end",), 
                    tag=("item", "product")
                )

                for event, elem in context:
                    # 1. Parse the element
                    record = self._parse_element(elem)
                    
                    # 2. Critical: Yield control back to the event loop 
                    # This prevents the scraper from "starving" the currency fetcher
                    await asyncio.sleep(0) 
                    
                    if record:
                        yield record
                    
                    # 3. Memory Cleanup
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

            except Exception as e:
                logger.error(f"[PRICE_SCRAPER] Fatal error: {e}")
                raise

    def _parse_element(self, elem: etree._Element) -> ProductPriceRecord | None:
        try:
            p_id = elem.findtext("id") or elem.findtext("sku")
            raw_price = elem.findtext("price")
            raw_ts = elem.findtext("timestamp") or elem.findtext("updated_at")

            if not all([p_id, raw_price, raw_ts]):
                return None

            return ProductPriceRecord(
                product_id=str(p_id),
                price=float(raw_price),
                currency=elem.findtext("currency") or "USD",
                timestamp=datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            )
        except (ValueError, ValidationError):
            return None

# --- COLLECTOR 2: THE CURRENCY FETCHER (ASYNC) ---

class AsyncCurrencyFetcher:
    """
    High-speed HTML scraper for FloatRates.com.
    """
    def __init__(self, target_url: str, user_agent: str = DEFAULT_USER_AGENT):
        self.target_url = target_url
        self.headers = {"User-Agent": user_agent}

    async def stream_rates(self, base_currency: str, interval: float) -> AsyncIterator[CurrencyRateRecord]:
        logger.info(f"[CURRENCY_FETCHER] Starting poll loop. Base: {base_currency}")
        
        while True:
            try:
                async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
                    response = await client.get(self.target_url)
                    response.raise_for_status()
                    
                    rates = self._parse_html(response.text, base_currency)
                    for target, rate in rates.items():
                        if target.upper() != base_currency.upper():
                            yield CurrencyRateRecord(
                                base_currency=base_currency,
                                target_currency=target,
                                rate=rate
                            )
                
                logger.info(f"[CURRENCY_FETCHER] Successful scrape. Sleeping {interval}s")
            except Exception as e:
                logger.error(f"[CURRENCY_FETCHER] Error: {e}. Retrying...")

            await asyncio.sleep(interval)

    def _parse_html(self, html_content: str, base_currency: str) -> dict[str, float]:
        parser = HTMLParser(html_content)
        table = parser.css_first("div.bk-rates-index table")
        if not table:
            raise ValueError("Rate table not found.")

        # 1. Map Columns
        column_map: dict[str, int] = {}
        header_cells = table.css("tr:first-child td")
        for idx, cell in enumerate(header_cells):
            link = cell.css_first("a")
            if link:
                code = link.text().strip().upper()
                if len(code) == 3:
                    column_map[code] = idx

        # 2. Find Base Row
        target_row = None
        for row in table.css("tr")[1:]:
            cells = row.css("td")
            if cells and cells[0].text().strip().upper() == base_currency.upper():
                target_row = row
                break
        
        if not target_row:
            raise ValueError(f"Base currency {base_currency} not in table.")

        # 3. Extract Intersection
        rates: dict[str, float] = {}
        target_cells = target_row.css("td")
        for code, col_idx in column_map.items():
            if col_idx < len(target_cells):
                text = target_cells[col_idx].text()
                match = re.search(r"(\d+\.\d+)", text)
                if match:
                    rates[code] = float(match.group(1))
        
        return rates

# --- UNIFIED ORCHESTRATION ENGINE ---

async def run_price_task(scraper: AsyncPriceScraper, url: str, broker: MessageBrokerClient):
    """Task wrapper for the Price Scraper."""
    records: list[ProductPriceRecord] = []
    try:
        async for record in scraper.stream_prices(url):
            records.append(record)
            await broker.send_price_data(record)
    except Exception as e:
        logger.critical(f"[PRICE_TASK] Crashed: {e}")
    return records

async def run_currency_task(fetcher: AsyncCurrencyFetcher, base: str, interval: float, broker: MessageBrokerClient):
    """Task wrapper for the Currency Fetcher."""
    records: list[CurrencyRateRecord] = []
    try:
        async for record in fetcher.stream_rates(base, interval):
            records.append(record)
            await broker.send_currency_data(record)
    except Exception as e:
        logger.critical(f"[CURRENCY_TASK] Crashed: {e}")
    return records

def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


async def run_delta_nexus_engine(
    price_url: str,
    currency_url: str = DEFAULT_CURRENCY_URL,
    output_dir: str | Path = ".",
):
    """
    The Unified Program Block. 
    Runs both collectors simultaneously without blocking.
    """
    logger.info("--- INITIALIZING DELTA NEXUS ENGINE ---")
    
    broker = MessageBrokerClient()
    price_scraper = AsyncPriceScraper()
    currency_fetcher = AsyncCurrencyFetcher(target_url=currency_url)

    price_records, rate_records = await asyncio.gather(
        run_price_task(price_scraper, price_url, broker),
        run_currency_task(currency_fetcher, DEFAULT_BASE_CURRENCY, CURRENCY_FETCH_INTERVAL, broker),
    )

    output_dir = Path(output_dir)
    price_rows = [record.model_dump(mode="json") for record in price_records or []]
    rate_rows = [record.model_dump(mode="json") for record in rate_records or []]

    _write_csv(output_dir / "scraped_product_prices.csv", price_rows, ["product_id", "price", "currency", "timestamp"])
    _write_csv(output_dir / "processed_currency_rates.csv", rate_rows, ["base_currency", "target_currency", "rate", "timestamp"])
    logger.info(f"Wrote CSV outputs to {output_dir.resolve()}")


if __name__ == "__main__":
    TEST_PRICE_URL = "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml"

    try:
        asyncio.run(run_delta_nexus_engine(TEST_PRICE_URL, DEFAULT_CURRENCY_URL))
    except KeyboardInterrupt:
        logger.info("Engine stopped by user.")
