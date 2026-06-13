import asyncio
import csv
from pathlib import Path

from loguru import logger

from ..broker import (
    KafkaProducerClient,
    MessageBrokerClient,
    ProductPriceRecord,
    CurrencyRateRecord,
)
from .scrape_exchange_rates import AsyncCurrencyFetcher
from .scrape_sitemaps import AsyncPriceScraper

DEFAULT_CURRENCY_URL: str = "https://www.floatrates.com/"
DEFAULT_BASE_CURRENCY: str = "USD"


async def run_price_task(scraper: AsyncPriceScraper, url: str, broker: MessageBrokerClient):
    """Task wrapper for the Price Scraper."""
    records: list[ProductPriceRecord] = []
    async for record in scraper.stream_prices(url):
        await broker.send_price_data(record)
        records.append(record)
    return records


async def run_currency_task(
    fetcher: AsyncCurrencyFetcher,
    base: str,
    broker: MessageBrokerClient,
):
    """Fetch and publish one currency-rate snapshot."""
    records: list[CurrencyRateRecord] = []
    for record in await fetcher.fetch_rates(base):
        await broker.send_currency_data(record)
        records.append(record)
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
    kafka_servers: str = "localhost:9092",
) -> None:
    """
    The Unified Program Block.
    Runs one product scrape and one currency-rate snapshot concurrently.

    Args:
        price_url: XML feed/sitemap URL
        currency_url: Currency page URL (defaults to FloatRates)
        output_dir: Where to write CSV files
        kafka_servers: Kafka bootstrap servers (e.g., "localhost:9092,localhost:9093")
    """
    logger.info("--- INITIALIZING DELTA NEXUS ENGINE ---")

    producer = KafkaProducerClient(bootstrap_servers=kafka_servers)
    await producer.connect()

    price_scraper = AsyncPriceScraper()
    currency_fetcher = AsyncCurrencyFetcher(target_url=currency_url)

    try:
        price_task = asyncio.create_task(
            run_price_task(price_scraper, price_url, producer)
        )
        rate_task = asyncio.create_task(
            run_currency_task(currency_fetcher, DEFAULT_BASE_CURRENCY, producer)
        )
        tasks = (price_task, rate_task)
        try:
            price_records, rate_records = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        output_dir = Path(output_dir)
        price_rows = [
            record.model_dump(mode="json") for record in price_records or []
        ]
        rate_rows = [
            record.model_dump(mode="json") for record in rate_records or []
        ]

        _write_csv(
            output_dir / "scraped_product_prices.csv",
            price_rows,
            [
                "product_id",
                "product_name",
                "product_url",
                "source_url",
                "price",
                "currency",
                "timestamp",
            ],
        )
        _write_csv(
            output_dir / "processed_currency_rates.csv",
            rate_rows,
            ["base_currency", "target_currency", "rate", "timestamp"],
        )
        logger.info(f"Wrote CSV outputs to {output_dir.resolve()}")
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    TEST_PRICE_URL = (
        "https://feeds.datafeedwatch.com/25986/"
        "cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml"
    )
    try:
        asyncio.run(run_delta_nexus_engine(TEST_PRICE_URL, DEFAULT_CURRENCY_URL))
    except KeyboardInterrupt:
        logger.info("Engine stopped by user.")
