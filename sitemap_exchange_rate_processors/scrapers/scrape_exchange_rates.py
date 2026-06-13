"""Exchange-rate scraping for Delta Nexus."""

import argparse
import asyncio
import re
from typing import AsyncIterator

import httpx
from loguru import logger
from selectolax.parser import HTMLParser

from ..broker import CurrencyRateRecord


DEFAULT_INTERVAL_SECONDS = 30.0
DEFAULT_BASE_CURRENCY = "USD"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class CurrencyFetcherError(Exception):
    """Base exception for currency fetch failures."""


class ProviderConnectionError(CurrencyFetcherError):
    """Raised when the currency provider cannot be reached."""


class ProviderDataError(CurrencyFetcherError):
    """Raised when the provider response cannot be parsed."""


class AsyncCurrencyFetcher:
    """Fetch one snapshot or continuously poll a FloatRates-style table."""

    def __init__(self, target_url: str, user_agent: str = DEFAULT_USER_AGENT):
        self.target_url = target_url
        self.headers = {"User-Agent": user_agent}

    async def fetch_rates(self, base_currency: str) -> list[CurrencyRateRecord]:
        """Fetch and parse one snapshot of currency rates."""
        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=15.0,
            ) as client:
                response = await client.get(self.target_url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ProviderConnectionError(
                f"Currency provider request failed: {exc}"
            ) from exc

        rates = self._parse_html(response.text, base_currency)
        return [
            CurrencyRateRecord(
                base_currency=base_currency.upper(),
                target_currency=target,
                rate=rate,
            )
            for target, rate in rates.items()
            if target != base_currency.upper()
        ]

    async def get_rates(self, base_currency: str) -> dict[str, float]:
        """Compatibility API returning a currency-to-rate mapping."""
        records = await self.fetch_rates(base_currency)
        return {record.target_currency: record.rate for record in records}

    async def stream_rates(
        self,
        base_currency: str,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncIterator[CurrencyRateRecord]:
        """Continuously fetch rates until cancelled or stopped."""
        logger.info(f"[CURRENCY_FETCHER] Starting poll loop. Base: {base_currency}")
        while stop_event is None or not stop_event.is_set():
            try:
                for record in await self.fetch_rates(base_currency):
                    yield record
            except CurrencyFetcherError as exc:
                logger.error(f"[CURRENCY_FETCHER] {exc}. Retrying in {interval}s")

            try:
                if stop_event:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                else:
                    await asyncio.sleep(interval)
            except asyncio.TimeoutError:
                pass

    def _parse_html(
        self,
        html_content: str,
        base_currency: str,
    ) -> dict[str, float]:
        parser = HTMLParser(html_content)
        table = parser.css_first("div.bk-rates-index table")
        if not table:
            raise ProviderDataError("Exchange-rate table not found")

        rows = table.css("tr")
        if len(rows) < 2:
            raise ProviderDataError("Exchange-rate table contains no data rows")

        column_map: dict[str, int] = {}
        for index, cell in enumerate(rows[0].css("td")):
            link = cell.css_first("a")
            if link:
                code = link.text().strip().upper()
                if len(code) == 3:
                    column_map[code] = index

        target_cells = None
        for row in rows[1:]:
            cells = row.css("td")
            if not cells:
                continue
            label = cells[0].css_first("a")
            label_text = label.text() if label else cells[0].text()
            if label_text.strip().upper() == base_currency.upper():
                target_cells = cells
                break

        if target_cells is None:
            raise ProviderDataError(
                f"Base currency '{base_currency}' not found in table"
            )

        rates: dict[str, float] = {}
        for code, column_index in column_map.items():
            if column_index >= len(target_cells):
                continue
            match = re.search(r"(\d+(?:\.\d+)?)", target_cells[column_index].text())
            if match:
                rates[code] = float(match.group(1))

        if not rates:
            raise ProviderDataError(
                f"No rates found for base currency '{base_currency}'"
            )
        return rates


# Retain the previous standalone class name for existing callers.
FloatRatesScraper = AsyncCurrencyFetcher


async def run_currency_fetcher(
    provider: AsyncCurrencyFetcher,
    base_currency: str = DEFAULT_BASE_CURRENCY,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    stop_event: asyncio.Event | None = None,
) -> AsyncIterator[CurrencyRateRecord]:
    """Backward-compatible continuous currency-fetcher entry point."""
    async for record in provider.stream_rates(
        base_currency,
        interval=interval,
        stop_event=stop_event,
    ):
        yield record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Nexus currency scraper")
    parser.add_argument("--url", required=True, help="FloatRates-style page URL")
    parser.add_argument("--base", default=DEFAULT_BASE_CURRENCY)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS)
    args = parser.parse_args()

    async def main() -> None:
        fetcher = AsyncCurrencyFetcher(args.url)
        async for rate_record in fetcher.stream_rates(args.base, args.interval):
            logger.info(rate_record.model_dump_json())

    asyncio.run(main())
