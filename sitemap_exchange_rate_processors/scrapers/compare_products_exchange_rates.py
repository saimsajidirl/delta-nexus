import asyncio
from datetime import datetime

from loguru import logger

from ..broker import (
    KafkaProducerClient,
    MessageBrokerClient,
    ProductPriceRecord,
    CurrencyRateRecord,
)
from .ali_express_scraper import ALIEXPRESS_URL, run_aliexpress_scraper
from .scrape_exchange_rates import AsyncCurrencyFetcher

DEFAULT_CURRENCY_URL: str = "https://www.floatrates.com/"
DEFAULT_BASE_CURRENCY: str = "USD"


async def run_price_task(broker: MessageBrokerClient, url: str = ALIEXPRESS_URL) -> list[ProductPriceRecord]:
    """Scrape AliExpress products and publish each as a ProductPriceRecord to Kafka."""
    logger.info("[PRICE_PIPELINE] Starting AliExpress scrape from {}", url)

    loop = asyncio.get_running_loop()
    payload = await loop.run_in_executor(None, lambda: run_aliexpress_scraper(url=url))

    scraped_at = datetime.fromisoformat(payload["scraped_at"])
    records: list[ProductPriceRecord] = []

    for i, product in enumerate(payload["products"], start=1):
        record = ProductPriceRecord(
            product_id=product["item_id"],
            product_name=product["title"],
            product_url=product["product_url"],
            source_url=url,
            price=product["price"],
            currency=product.get("currency", "USD"),
            timestamp=scraped_at,
        )
        await broker.send_price_data(record)
        records.append(record)
        if i <= 5 or i % 10 == 0:
            logger.info(
                "[PRICE_PIPELINE] Published product #{}: id={} name={!r} price={} {}",
                i,
                record.product_id,
                record.product_name,
                record.price,
                record.currency,
            )

    logger.info("[PRICE_PIPELINE] Finished. Published {} product events.", len(records))
    return records


async def run_currency_task(
    fetcher: AsyncCurrencyFetcher,
    base: str,
    broker: MessageBrokerClient,
) -> list[CurrencyRateRecord]:
    """Fetch and publish one currency-rate snapshot."""
    logger.info("[FX_PIPELINE] Fetching currency-rate snapshot. Base={}", base)
    records: list[CurrencyRateRecord] = []
    for record in await fetcher.fetch_rates(base):
        await broker.send_currency_data(record)
        records.append(record)
        count = len(records)
        if count <= 5 or count % 25 == 0:
            logger.info(
                "[FX_PIPELINE] Published FX rate #{}: {}/{} = {}",
                count,
                record.base_currency,
                record.target_currency,
                record.rate,
            )
    logger.info("[FX_PIPELINE] Finished FX snapshot. Published {} currency events.", len(records))
    return records


async def run_delta_nexus_engine(
    aliexpress_url: str = ALIEXPRESS_URL,
    currency_url: str = DEFAULT_CURRENCY_URL,
    kafka_servers: str = "localhost:9092",
) -> None:
    """
    Unified pipeline: scrape AliExpress products and one FX snapshot concurrently,
    publish both to Kafka. The Kafka consumer persists all events to PostgreSQL.

    Args:
        aliexpress_url: AliExpress page to scrape (defaults to homepage)
        currency_url:   FloatRates-style currency page URL
        kafka_servers:  Kafka bootstrap servers
    """
    logger.info("--- INITIALIZING DELTA NEXUS ENGINE ---")

    producer = KafkaProducerClient(bootstrap_servers=kafka_servers)
    await producer.connect()

    currency_fetcher = AsyncCurrencyFetcher(target_url=currency_url)

    try:
        price_task = asyncio.create_task(run_price_task(producer, aliexpress_url))
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

        logger.info(
            "Engine finished — published {} product and {} FX events to Kafka. "
            "Kafka consumer will persist them to PostgreSQL.",
            len(price_records),
            len(rate_records),
        )
    finally:
        await producer.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(run_delta_nexus_engine())
    except KeyboardInterrupt:
        logger.info("Engine stopped by user.")
