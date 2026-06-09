import asyncio
import re
from abc import ABC, abstractmethod
from typing import Any

import httpx
from loguru import logger
from pydantic import BaseModel, Field, ValidationError
from selectolax.parser import HTMLParser

# --- CONSTANTS ---
DEFAULT_INTERVAL_SECONDS: float = 30.0
DEFAULT_BASE_CURRENCY: str = "USD"
DEFAULT_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# --- EXCEPTIONS ---

class CurrencyFetcherError(Exception):
    """Base exception for currency fetching operations."""
    pass

class ProviderConnectionError(CurrencyFetcherError):
    """Raised when the website is unreachable."""
    pass

class ProviderDataError(CurrencyFetcherError):
    """Raised when the table structure is not found or is malformed."""
    pass

# --- DATA MODELS ---

class CurrencyRateRecord(BaseModel):
    """
    A validated record of an exchange rate.
    """
    base_currency: str = Field(..., min_length=3, max_length=3)
    target_currency: str = Field(..., min_length=3, max_length=3)
    rate: float = Field(..., gt=0)
    timestamp: Any = Field(default_factory=lambda: None) # Set during orchestration

# --- PROVIDER INTERFACE ---

class CurrencyProvider(BaseModel, ABC):
    """
    Abstract Base Class for Currency Providers.
    """
    @abstractmethod
    async def get_rates(self, base_currency: str) -> dict[str, float]:
        pass

# --- FLOATRATES SPECIFIC IMPLEMENTATION ---

class FloatRatesScraper:
    """
    A high-performance scraper using selectolax to parse the FloatRates matrix table.
    """
    def __init__(self, target_url: str, user_agent: str = DEFAULT_USER_AGENT):
        self.target_url = target_url
        self.headers = {"User-Agent": user_agent}

    async def get_rates(self, base_currency: str) -> dict[str, float]:
        """
        Fetches the HTML and extracts the specific row for the base_currency.
        """
        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                response = await client.get(self.target_url)
                response.raise_for_status()
                return self._parse_matrix(response.text, base_currency)
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error: {e.response.status_code}")
                raise ProviderConnectionError(f"Website unreachable: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during scraping: {e}")
                raise ProviderConnectionError(f"Scraping failed: {e}")

    def _parse_matrix(self, html_content: str, base_currency: str) -> dict[str, float]:
        """
        Parses the HTML table into a dictionary of rates relative to the base_currency.
        """
        parser = HTMLParser(html_content)
        
        # 1. Find the table. Looking for the class specified in your HTML snippet.
        table = parser.css_first("div.bk-rates-index table")
        if not table:
            raise ProviderDataError("Could not find the exchange rate table in the HTML.")

        rows = table.css("tr")
        if len(rows) < 2:
            raise ProviderDataError("Table found, but contains insufficient data rows.")

        # 2. Map Column Headers
        # The first row contains the currency codes in <td> tags.
        # Index 0 is usually empty/spacer, so we offset.
        header_row = rows[0]
        header_cells = header_row.css("td")
        
        # column_map: { "EUR": 2, "GBP": 3 ... }
        column_map: dict[str, int] = {}
        for idx, cell in enumerate(header_cells):
            # Extract text from the <a> tag inside the <td>
            link = cell.css_first("a")
            if link:
                currency_code = link.text().strip().upper()
                if len(currency_code) == 3:
                    column_map[currency_code] = idx

        # 3. Find the Data Row for the requested Base Currency
        target_row: Any = None
        for row in rows[1:]:  # Skip header row
            cells = row.css("td")
            if not cells:
                continue
            
            # The first cell in the row is the currency name/link
            row_label_node = cells[0].css_first("a")
            if row_label_node:
                row_label = row_label_node.text().strip().upper()
                if row_label == base_currency.upper():
                    target_row = row
                    break
        
        if not target_row:
            raise ProviderDataError(f"Base currency '{base_currency}' not found in table rows.")

        # 4. Extract Rates from the target row
        results: dict[str, float] = {}
        target_cells = target_row.css("td")

        for currency_code, col_idx in column_map.items():
            # We don't want to return the rate of USD to USD (which is 1) 
            # unless it's requested, but typically we want all cross-rates.
            if col_idx < len(target_cells):
                cell_text = target_cells[col_idx].text()
                # Use regex to grab only the numeric part (ignoring the chart icon text/links)
                match = re.search(r"(\d+\.\d+)", cell_text)
                if match:
                    results[currency_code] = float(match.group(1))
                else:
                    logger.warning(f"Could not parse numeric value for {currency_code} in row {base_currency}")

        if not results:
            raise ProviderDataError(f"Failed to extract any rates for base '{base_currency}'")

        return results

# --- ORCHESTRATION ENGINE ---

async def run_currency_fetcher(
    provider: FloatRatesScraper,
    base_currency: str = DEFAULT_BASE_CURRENCY,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    stop_event: asyncio.Event | None = None
) -> asyncio.Generator[CurrencyRateRecord, None, None]:
    """
    The primary entry-point that runs the continuous loop.
    """
    logger.info(f"Starting Delta Nexus currency fetcher. Base: {base_currency}")
    
    while stop_event is None or not stop_event.is_set():
        try:
            rates_map = await provider.get_rates(base_currency)

            for target_currency, rate_value in rates_map.items():
                try:
                    # We skip the self-rate (e.g., USD to USD) to save bandwidth/processing
                    if target_currency == base_currency.upper():
                        continue
                        
                    yield CurrencyRateRecord(
                        base_currency=base_currency,
                        target_currency=target_currency,
                        rate=rate_value
                    )
                except ValidationError as ve:
                    logger.warning(f"Validation error for {target_currency}: {ve}")
                    continue

            logger.info(f"Scrape cycle complete. Extracted {len(rates_map)} rates.")

        except ProviderConnectionError as ce:
            logger.error(f"Network error: {ce}. Retrying in {interval}s...")
        except ProviderDataError as de:
            logger.error(f"Data error (Table layout changed?): {de}")
        except Exception as e:
            logger.exception(f"Unexpected error in fetcher loop: {e}")
        
        try:
            if stop_event:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            else:
                await asyncio.sleep(interval)
        except asyncio.TimeoutError:
            pass

# --- CLI ENTRY POINT ---

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Delta Nexus - FloatRates Scraper")
    parser.add_argument("--url", type=str, required=True, help="Target FloatRates URL")
    parser.add_argument("--base", type=str, default=DEFAULT_BASE_CURRENCY, help="Base currency (e.g. USD)")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS, help="Polling interval")

    args = parser.parse_args()

    async def main_execution():
        # Initialize the Selectolax-based Scraper
        scraper = FloatRatesScraper(target_url=args.url)
        stop_signal = asyncio.Event()

        try:
            async for record in run_currency_fetcher(
                provider=scraper,
                base_currency=args.base,
                interval=args.interval,
                stop_event=stop_signal
            ):
                # In Task 2, this is sent to the Message Broker (Kafka/RabbitMQ)
                logger.info(f"NEW RATE: {record.base_currency} -> {record.target_currency} = {record.rate}")
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user.")
            stop_signal.set()
        except Exception as e:
            logger.critical(f"Fatal system error: {e}")

    asyncio.run(main_execution())
