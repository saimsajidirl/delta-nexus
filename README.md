# Delta Nexus

Delta Nexus is a learning-focused Python data pipeline that fetches and processes data from two public sources:

1. **Product data**: XML feeds or sitemap URLs
2. **Currency rates**: Live exchange rate pages (FloatRates-style HTML)

The pipeline decouples data collection from processing using Kafka as its required message path.
If Kafka cannot be reached or a message cannot be published, the engine raises an error and the run fails.

> **Project status:** Delta Nexus is an evolving portfolio and learning project. It demonstrates async collection, validated event contracts, Kafka integration, and CSV persistence, but it is not yet production-ready. Production use still requires durable storage, retries and dead-letter handling, observability, security, deployment automation, and broader integration/load testing.

## Quick Start

### Kafka Producer

```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
        kafka_servers="kafka-broker.example.com:9092"
    )
)
```

Kafka decouples collection from downstream processing and allows independent consumer groups. The current project is a single-node development setup, not a benchmarked real-time deployment.

## Architecture

### High-Level Flow

```text
Scrapers
   |
   v
KafkaProducerClient
   |
   +--> raw-prices
   |
   `--> fx-rates
           |
           v
       Consumers
       - CSV writer
       - Database
       - Search index
       - Analytics
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
│   ├── kafka_producer.py            # Kafka producer implementation
│   └── kafka_consumer.py            # Kafka consumer + CSV writer
└── __init__.py                      # Package exports
```

## Components

### Scrapers (`scrapers/`)

#### `scrape_sitemaps.py` — AsyncPriceScraper

Downloads an XML feed and parses its product elements incrementally using `lxml.etree.iterparse()`. The parser clears processed nodes, although the HTTP response is currently buffered before parsing.

**Features:**
- Extracts `product_id`, `product_name`, `product_url`, `price`, `currency`, `timestamp`
- Flexible field mapping: recognizes `id`, `sku`, `item_id`, `product_id`
- Clears parsed XML nodes to limit parser memory growth
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

Imports the canonical scraper classes, orchestrates them asynchronously, and routes records through the message broker. Scraping and parsing logic remains in the source-specific modules.

**Pipeline:**
1. Start XML scraper task
2. Fetch one currency-rate snapshot
3. Validate records as they arrive
4. Publish each record to Kafka
5. Write CSV output and finish the run

### Broker (`broker/`)

The broker abstraction decouples scrapers from output sinks, enabling multiple backends without changing scraper code.

#### `broker.py` — Abstract Interface

Defines:
- `MessageBrokerClient` (ABC)
- `ProductPriceRecord` (Pydantic model)
- `CurrencyRateRecord` (Pydantic model)

#### `kafka_producer.py` — KafkaProducerClient

Sends records to Kafka topics (default topics: `raw-prices`, `fx-rates`).

**Methods:**
- `connect()` — Initialize producer
- `disconnect()` — Close producer
- `send_price_data(record)` — Send to `raw-prices` topic
- `send_currency_data(record)` — Send to `fx-rates` topic

#### `kafka_consumer.py` — KafkaConsumerWriter

Consumes records from Kafka and writes to CSV.

**Methods:**
- `start_consuming(timeout_seconds)` — Listen and write
- `_write_csv_files()` — Persist records to disk

## Usage

### Kafka Producer + Consumer

Scraper sends records to Kafka; consumer processes messages independently. Separates concerns and enables scaling.

**Producer (sends data to Kafka):**
```python
import asyncio
from sitemap_exchange_rate_processors import run_delta_nexus_engine

asyncio.run(
    run_delta_nexus_engine(
        price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml",
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
| `kafka_servers` | str | `localhost:9092` | Kafka bootstrap servers |

**Returns:** `None`. The engine raises if Kafka connection or publishing fails.

**Example:**
```python
await run_delta_nexus_engine(
    price_url="https://example.com/products.xml",
    output_dir="./data"
)
```

### `consume_from_kafka(...)`

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrap_servers` | str | `localhost:9092` | Kafka broker address |
| `price_topic` | str | `raw-prices` | Topic for product data |
| `rate_topic` | str | `fx-rates` | Topic for rates |
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

# Kafka support is required
pip install aiokafka
```

**Requirements:**
- Python 3.9+
- `httpx` — async HTTP client
- `lxml` — XML parsing
- `selectolax` — HTML parsing
- `pydantic` — data validation
- `aiokafka` — required asynchronous Kafka client

## Kafka Deployment

Delta Nexus expects Kafka to be running and accessible at the specified `bootstrap_servers` URL.

**Topics:**
- `raw-prices` — Product price records (JSON)
- `fx-rates` — Exchange rate records (JSON)

### Event Contract

Kafka messages use a versioned JSON envelope with these shared fields:

- `schema_version` - currently `1.0`
- `event_type` - `product_price` or `currency_rate`
- `emitted_at` - UTC timestamp when the event was published

Required fields for product price events:

- `product_id`
- `price`
- `timestamp`

Optional fields for product price events:

- `product_name`
- `product_url`
- `source_url`
- `currency`

Required fields for FX rate events:

- `base_currency`
- `target_currency`
- `rate`
- `timestamp`

Compatibility rules:

- Consumers must accept messages with the current `schema_version` only.
- New optional fields may be added without breaking existing consumers.
- Required fields should not be removed or renamed without bumping `schema_version`.
- Topic names are stable contract names and should be updated in producer and consumer together.

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

await run_delta_nexus_engine(
    price_url="https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml"
)
```

You can also test individual components:

```python
from sitemap_exchange_rate_processors.scrapers import AsyncPriceScraper, AsyncCurrencyFetcher

# Test price scraper
scraper = AsyncPriceScraper()
async for record in scraper.stream_prices("https://example.com/products.xml"):
    print(record)

# Test currency fetcher
fetcher = AsyncCurrencyFetcher("https://www.floatrates.com/")
rates = await fetcher.fetch_rates("USD")
```

## Output Formats

The engine writes local CSV snapshots after all records have been published to
Kafka successfully. The separate CSV consumer can also write consumed events:

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

- Delta Nexus is a learning prototype and should not yet be treated as production-ready
- Live URLs are required; feeds are not bundled
- CSV files are overwritten each run (consider timestamping in production)
- Kafka integration uses `aiokafka` for async support
