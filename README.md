# Delta Nexus

Delta Nexus is a Python scraping pipeline for two public data sources:

1. product or sitemap XML feeds
2. live currency exchange rate pages

The code is organized so the same data can be fetched, parsed, validated, logged, and exported to CSV from one unified runner.

## Project layout

- `scrape_sitemaps.py`
  - streams XML records one element at a time
  - extracts product fields
  - validates them into `ProductPriceRecord`
- `scrape_exchange_rates.py`
  - fetches a FloatRates-style HTML page
  - parses the exchange-rate table
  - yields `CurrencyRateRecord` objects in a polling loop
- `compare_sitemap_exchange_rates.py`
  - runs both collectors together
  - sends each validated record through the broker stub
  - writes the collected rows to CSV at the end of the run

## Main flow

If you run the unified entrypoint `run_delta_nexus_engine(...)`, Delta Nexus does the whole pipeline:

1. start the XML scraper task
2. start the currency fetcher task
3. validate and collect records from both streams
4. send each record through the broker stub
5. write the final outputs to CSV files

The current output files are:

- `scraped_product_prices.csv`
- `processed_currency_rates.csv`

These files are written into the output directory you pass to the function. If you do not pass one, they are written in the current working directory.

## Entry point

The unified function is:

```python
await run_delta_nexus_engine(price_url, currency_url, output_dir)
```

Parameters:

- `price_url`: a public XML feed or sitemap URL
- `currency_url`: a FloatRates-style HTML page, defaulting to `https://www.floatrates.com/daily.html`
- `output_dir`: folder where the CSV files will be saved

Example:

```python
import asyncio
from compare_sitemap_exchange_rates import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        "https://www.floatrates.com/daily.html",
        output_dir="outputs"
    )
)
```

## What each part does

### XML scraper

The XML scraper uses `lxml.etree.iterparse()` so it can process records incrementally instead of loading the whole file into memory. For each matching XML element, it tries to read:

- `id`, `sku`, `item_id`, or `product_id`
- `price`
- `timestamp`, `updated_at`, `last_updated`, or `pubDate`
- `currency`

If a record passes validation, it becomes a `ProductPriceRecord`. The code also clears parsed XML nodes so memory stays under control while the stream is processed.

### Currency scraper

The currency scraper uses `httpx` to fetch the page and `selectolax` to read the rate table. It then:

- finds the currency codes in the table header
- locates the row for the chosen base currency
- extracts numeric rate values from the row

Each parsed rate becomes a `CurrencyRateRecord`.

### Unified engine

`run_delta_nexus_engine(...)` creates both collectors and runs them together with `asyncio.gather()`. The price scraper and rate scraper work at the same time, and each record is passed to the broker stub before being written to CSV.

Right now the broker stub only logs records. That keeps the architecture ready for Kafka, RabbitMQ, or a database sink later without changing the scraping flow.

## Testing notebook

Use `Delta_Nexus_Testing.ipynb` to test the real flow on live public websites. The notebook:

- imports the project modules from `Code/`
- pulls a live XML feed
- runs `run_price_scraper(...)`
- pulls the live FloatRates page
- runs `FloatRatesScraper.get_rates(...)`
- can also call `run_delta_nexus_engine(...)` directly so you can test the full pipeline in one place

## Notes

- The project name has been updated to Delta Nexus in the logs and documentation.
- The code is still a prototype, so live URLs must be supplied when you run it.
- The output CSV files are overwritten each time the unified runner completes.
