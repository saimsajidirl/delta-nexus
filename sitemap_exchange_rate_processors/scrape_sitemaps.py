import argparse
import os
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union
from lxml import etree
from pydantic import BaseModel, Field, ValidationError
from loguru import logger

DEFAULT_USER_AGENT: str = "DeltaNexus/1.0 (Price Scraper Engine)"
DEFAULT_TIMEOUT: int = 30
NS_MAP: dict[str, str] = {} 


def _local_name(tag: object) -> str:
    """Return the XML local name without namespace noise."""
    text = str(tag)
    if "}" in text:
        return text.rsplit("}", 1)[1]
    return text.split(":", 1)[-1]

class ScraperError(Exception):
    """Base exception for all scraper-related errors."""
    pass

class ScraperIOError(ScraperError):
    """Raised when file access or network retrieval fails."""
    pass

class ScraperParsingError(ScraperError):
    """Raised when the XML structure is malformed or unreadable."""
    pass

class DataValidationError(ScraperError):
    """Raised when extracted data does not meet the required schema."""
    pass

# --- DATA MODELS ---

class ProductPriceRecord(BaseModel):
    """
    Strict schema for a single scraped product record.
    Validates that prices are positive and IDs are non-empty.
    """
    product_id: str = Field(..., min_length=1)
    product_name: str | None = None
    product_url: str | None = None
    source_url: str | None = None
    price: float = Field(..., gt=0)
    currency: str = Field(default="USD")
    timestamp: datetime

# --- CORE LOGIC ---

def _stream_xml_elements(source: Union[str, Path]) -> Iterator[etree._Element]:
    """
    An event-driven generator that streams XML elements one by one.
    
    Uses lxml.etree.iterparse to ensure that we do not load the entire 
    XML tree into memory, satisfying the low-memory requirement.
    
    Args:
        source: The file path or URL string to stream from.

    Yields:
        etree._Element: The next parsed XML element.

    Raises:
        ScraperIOError: If the source cannot be opened.
        ScraperParsingError: If the XML is syntactically invalid.
    """
    try:
        # 'end' event ensures we only process the element once it's fully parsed
        context = etree.iterparse(str(source), events=("end",), recover=False)
        
        for event, elem in context:
            if _local_name(elem.tag) in ("item", "product", "url"):
                yield elem

                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                    
    except etree.XMLSyntaxError as e:
        logger.error(f"Malformed XML detected in {source}: {e}")
        raise ScraperParsingError(f"Failed to parse XML: {e}")
    except (IOError, OSError) as e:
        logger.error(f"Failed to access source {source}: {e}")
        raise ScraperIOError(f"IO failure during streaming: {e}")

def _parse_element_to_record(element: etree._Element, source_path: Union[str, Path] | None = None) -> ProductPriceRecord | None:
    """
    Maps XML sub-elements to the ProductPriceRecord dataclass.
    
    This function handles the logic of finding specific tags (id, price, etc.)
    within the current XML node.
    
    Args:
        element: The XML element representing a single product.

    Returns:
        ProductPriceRecord: A validated record if successful.
        None: If required fields are missing or data is invalid.
    """
    try:
        def first_text(*names: str) -> str | None:
            for child in element.iterchildren():
                if _local_name(child.tag) in names and child.text:
                    return child.text.strip()
            for name in names:
                value = element.findtext(name)
                if value:
                    return value.strip()
            return None

        p_id = first_text("id", "sku", "item_id", "product_id")
        p_name = first_text("title", "name", "product_name")
        p_url = first_text("link", "url", "loc", "product_url")
        raw_price = first_text("price", "g:price")
        raw_ts = first_text("timestamp", "updated_at", "last_updated", "pubDate")
        currency = first_text("currency") or "USD"

        if not all([p_id, raw_price]):
            logger.debug(f"Skipping element: Missing required fields (ID: {p_id}, Price: {raw_price})")
            return None

        price_parts = str(raw_price).split()
        price_value = price_parts[0]
        if len(price_parts) > 1 and currency == "USD":
            currency = price_parts[-1]

        return ProductPriceRecord(
            product_id=str(p_id),
            product_name=p_name,
            product_url=p_url,
            source_url=str(source_path) if source_path else None,
            price=float(price_value),
            currency=currency,
            timestamp=datetime.fromisoformat(raw_ts.replace("Z", "+00:00")) if raw_ts else datetime.utcnow()
        )

    except (ValueError, TypeError, ValidationError) as e:
        logger.warning(f"Data validation failed for element {element.tag}: {e}")
        return None

def run_price_scraper(source_path: Union[str, Path]) -> Iterator[ProductPriceRecord]:
    """
    The primary domain entry-point for the Price Scraper.
    
    Orchestrates the streaming of the XML file and the conversion of 
    raw elements into validated ProductPriceRecord objects.

    Args:
        source_path: The local path or URL of the XML sitemap/feed.

    Yields:
        ProductPriceRecord: Validated product data ready for the Broker.
    """
    logger.info(f"Starting Price Scraper on source: {source_path}")
    
    count = 0
    success_count = 0

    for element in _stream_xml_elements(source_path):
        count += 1
        record = _parse_element_to_record(element, source_path)
        
        if record:
            success_count += 1
            yield record

    logger.info(f"Scrape completed. Processed: {count} | Valid Records: {success_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Nexus - High-Efficiency Price Scraper")
    parser.add_argument(
        "--source", 
        type=str, 
        required=True, 
        help="Path to the XML sitemap or product feed"
    )
    parser.add_argument(
        "--output-log", 
        type=str, 
        default="scraper.log", 
        help="File to save logs to"
    )

    args = parser.parse_args()

    # Configure Loguru
    logger.add(args.output_log, rotation="500 MB", retention="10 days")

    try:
        # Execute the domain entry-point
        for product_record in run_price_scraper(Path(args.source)):
            # In Task 2, this is where we will hand off to the Message Broker.
            # For now, we simply log the successful extraction.
            logger.debug(f"Extracted: {product_record.json()}")
            
    except ScraperError as e:
        logger.critical(f"Scraper halted due to a critical error: {e}")
        exit(1)
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user.")
        exit(0)
    except Exception as e:
        logger.exception(f"An unexpected system error occurred: {e}")
        exit(1)
