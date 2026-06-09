# Delta Nexus

Delta Nexus is a production-ready Python scraping pipeline that fetches and processes data from two public sources:

1. **Product data**: XML feeds or sitemap URLs
2. **Currency rates**: Live exchange rate pages (FloatRates-style HTML)

The pipeline decouples data collection from processing using an abstract message broker that supports two modes:
- **Stub broker** (default): Direct CSV output with zero external dependencies
- **Kafka broker**: Scalable, real-time processing with support for multiple consumers

## Quick Start

### Stub Mode (No External Dependencies)

```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        currency_url="https://www.floatrates.com/daily.html"
    )
)
```

Records are logged and written to CSV files in the specified output directory.

### Kafka Mode (Scalable Real-Time)

```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        use_kafka=True,
        kafka_servers="kafka-broker.example.com:9092"
    )
)
```

Kafka enables decoupled, real-time processing with support for multiple consumers processing the same data independently.

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────┐
│      Delta Nexus Scrapers               │
│  - XML Product Scraper (sitemap)        │
│  - Currency Rate Fetcher (HTML table)   │
└──────────────┬──────────────────────────┘
               │ (records)
               ▼
    ┌──────────────────────┐
    │  Message Broker      │
    │  (Abstract Interface)│
    └──┬──────────────┬────┘
       │              │
   (Stub)        (Kafka)
       │              │
       ▼              ▼
    CSV Files    Kafka Topics
    (immediate)  (async)
                    │
                    ▼
            ┌───────────────┐
            │ Consumers     │
            │ - CSV Writer  │
            │ - Database    │
            │ - Alerts      │
            │ - Analytics   │
            └───────────────┘
```

### Project Structure

```
sitemap_exchange_rate_processors/
├── scrapers/
│   ├── scrape_sitemaps.py           # XML parsing + validation
│   ├── scrape_exchange_rates.py     # HTML table parsing
│   └── compare_sitemap_exchange_rates.py  # Unified orchestrator
├── broker/
│   ├── broker.py                    # Abstract broker interface
│   ├── kafka_broker.py              # Kafka producer implementation
│   └── kafka_consumer.py            # Kafka consumer + CSV writer
└── __init__.py                      # Package exports
```

## Components

### Scrapers (`scrapers/`)

#### `scrape_sitemaps.py` — AsyncPriceScraper

Streams XML records incrementally using `lxml.etree.iterparse()` to handle large feeds without loading everything into memory.

**Features:**
- Extracts `product_id`, `product_name`, `product_url`, `price`, `currency`, `timestamp`
- Flexible field mapping: recognizes `id`, `sku`, `item_id`, `product_id`
- Memory-efficient: clears parsed nodes as it streams
- Validates data into `ProductPriceRecord` schema (Pydantic)

#### `scrape_exchange_rates.py` — AsyncCurrencyFetcher

Fetches and parses HTML exchange rate tables.

**Features:**
- Uses `httpx` for async HTTP fetching
- Uses `selectolax` for fast HTML parsing
- Extracts currency codes from table headers
- Parses rates into `CurrencyRateRecord` schema
- Customizable base currency (default: USD)

#### `compare_sitemap_exchange_rates.py` — run_delta_nexus_engine

Orchestrates both scrapers asynchronously and routes records through the message broker.

**Pipeline:**
1. Start XML scraper task
2. Start currency fetcher task
3. Validate records as they arrive
4. Send to broker (stub or Kafka)
5. Return completion stats

### Broker (`broker/`)

The broker abstraction decouples scrapers from output sinks, enabling multiple backends without changing scraper code.

#### `broker.py` — Abstract Interface

Defines:
- `MessageBrokerClient` (ABC)
- `ProductPriceRecord` (Pydantic model)
- `CurrencyRateRecord` (Pydantic model)

#### `kafka_broker.py` — KafkaMessageBrokerClient

Sends records to Kafka topics (default topics: `product-prices`, `currency-rates`).

**Methods:**
- `connect()` — Initialize producer
- `disconnect()` — Close producer
- `send_price_data(record)` — Send to `product-prices` topic
- `send_currency_data(record)` — Send to `currency-rates` topic

#### `kafka_consumer.py` — KafkaConsumerWriter

Consumes records from Kafka and writes to CSV.

**Methods:**
- `start_consuming(timeout_seconds)` — Listen and write
- `_write_csv_files()` — Persist records to disk

## Usage

### Mode 1: Stub Broker (Default)

Records are logged and written to CSV immediately. Perfect for learning and one-off runs.

```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        currency_url="https://www.floatrates.com/daily.html",
        use_kafka=False  # default
    )
)
```

**Output:**
- `scraped_product_prices.csv` — Product records
- `processed_currency_rates.csv` — Exchange rate records

### Mode 2: Kafka Producer + Consumer

Scraper sends records to Kafka; consumer processes messages independently. Separates concerns and enables scaling.

**Producer (sends data to Kafka):**
```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        use_kafka=True,
        kafka_servers="kafka-broker.example.com:9092"
    )
)
```

**Consumer (processes records from Kafka):**
```python
import asyncio
from sitemap_exchange_rate_processors import consume_from_kafka

asyncio.run(
    consume_from_kafka(
        bootstrap_servers="kafka-broker.example.com:9092",
        timeout_seconds=120
    )
)
```

## API Reference

### `run_delta_nexus_engine(...)`

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `price_url` | str | required | XML feed or sitemap URL |
| `currency_url` | str | `https://www.floatrates.com/daily.html` | Exchange rate page URL |
| `output_dir` | str | `.` (current dir) | Where to write CSV files |
| `use_kafka` | bool | `False` | Enable Kafka mode |
| `kafka_servers` | str | `localhost:9092` | Kafka bootstrap servers |

**Returns:** `dict` with keys `prices_count`, `rates_count`, `processing_time_seconds`

**Example:**
```python
result = await run_delta_nexus_engine(
    price_url="https://example.com/products.xml",
    output_dir="./data"
)
print(f"Processed {result['prices_count']} products")
```

### `consume_from_kafka(...)`

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrap_servers` | str | `localhost:9092` | Kafka broker address |
| `price_topic` | str | `product-prices` | Topic for product data |
| `rate_topic` | str | `currency-rates` | Topic for rates |
| `output_dir` | str | `.` | Where to write CSVs |
| `timeout_seconds` | int | `60` | Max wait for messages |

**Example:**
```python
await consume_from_kafka(
    bootstrap_servers="kafka.prod:9092",
    output_dir="./outputs",
    timeout_seconds=300
)
```

## Data Schemas

### ProductPriceRecord

```python
{
    "product_id": str,              # Required (min 1 char)
    "product_name": str | None,     # Optional
    "product_url": str | None,      # Optional
    "source_url": str | None,       # Optional
    "price": float,                 # Required (> 0)
    "currency": str,                # Default: "USD"
    "timestamp": datetime            # ISO 8601
}
```

### CurrencyRateRecord

```python
{
    "base_currency": str,           # e.g., "USD"
    "target_currency": str,         # e.g., "EUR"
    "rate": float,                  # Exchange rate
    "timestamp": datetime            # ISO 8601 (default: now)
}
```

## Installation

```bash
# Install package
pip install -r requirements.txt

# For Kafka mode, ensure aiokafka is installed
pip install aiokafka
```

**Requirements:**
- Python 3.9+
- `httpx` — async HTTP client
- `lxml` — XML parsing
- `selectolax` — HTML parsing
- `pydantic` — data validation
- `aiokafka` — Kafka client (optional, for Kafka mode)

## Kafka Deployment

Delta Nexus expects Kafka to be running and accessible at the specified `bootstrap_servers` URL.

**Topics:**
- `product-prices` — Product price records (JSON)
- `currency-rates` — Exchange rate records (JSON)

**Deployment options:**
- **Self-managed**: Kafka cluster on-premises or cloud VMs
- **Managed services**: Confluent Cloud, AWS MSK, Azure Event Hubs, Google Cloud Pub/Sub
- **Development**: Local Kafka instances via Docker or similar

Ensure the broker and consumer have network access to your Kafka servers and topics are auto-created or pre-created.

## Testing

Test the pipeline against live public data sources:

```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

# Test stub mode
await run_delta_nexus_engine(
    price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml"
)
```

You can also test individual components:

```python
from sitemap_exchange_rate_processors.scrapers import AsyncPriceScraper, AsyncCurrencyFetcher

# Test price scraper
scraper = AsyncPriceScraper()
async for record in scraper.scrape("https://example.com/products.xml"):
    print(record)

# Test currency fetcher
fetcher = AsyncCurrencyFetcher()
rates = await fetcher.fetch_rates()
```

## Choosing Stub vs Kafka

| Scenario | Mode | Why |
|----------|------|-----|
| Local development | Stub | Zero dependencies, instant CSV |
| One-time data export | Stub | Simple, no infrastructure |
| Production pipeline | Kafka | Scalable, decoupled, real-time |
| Multiple consumers | Kafka | Each runs independently |
| High-volume data | Kafka | Async, fault-tolerant |

## Output Formats

When using stub mode or CSV consumer, records are written to CSV files:

**Product prices:**
```
product_id,product_name,product_url,source_url,price,currency,timestamp
```

**Currency rates:**
```
base_currency,target_currency,rate,timestamp
```

In production, consider versioning output files with timestamps or checksums to preserve history.

## Extending Delta Nexus

### Custom Broker

Implement `MessageBrokerClient` to send records to any sink:

```python
from sitemap_exchange_rate_processors import MessageBrokerClient, ProductPriceRecord

class DatabaseBroker(MessageBrokerClient):
    async def send_price_data(self, record: ProductPriceRecord) -> None:
        # Write to your database
        pass

    async def send_currency_data(self, record: CurrencyRateRecord) -> None:
        # Write to your database
        pass
```

### Custom Consumer

Extend `KafkaConsumerWriter` or implement a new consumer to handle Kafka messages differently.

## Troubleshooting

**"Connection refused" (Kafka)**
- Verify Kafka broker is running and accessible at the specified `bootstrap_servers` address
- Check network connectivity and firewall rules
- Ensure broker port (default 9092) is exposed

**No records produced**
- Verify the source URL is accessible and returns valid data
- Check scraper logs for validation errors
- Ensure records match schema expectations

**Consumer timeout with no messages**
- Increase `timeout_seconds` parameter
- Verify producer has sent records (check application logs)
- Verify topic names match between producer and consumer
- Check Kafka broker logs for errors

## License

See LICENSE file for details.

## Notes

- Delta Nexus is a prototype but production-ready for single-threaded scraping
- Live URLs are required; feeds are not bundled
- CSV files are overwritten each run (consider timestamping in production)
- Kafka integration uses `aiokafka` for async support
