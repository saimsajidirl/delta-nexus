"""AliExpress browser scraper — stealth fetch, parse, and normalize product listings."""

import argparse
import json
import re
from datetime import datetime, timezone

from botasaurus.browser import Driver, browser
from botasaurus.soupify import soupify
from botasaurus.user_agent import UserAgent
from botasaurus.window_size import WindowSize
from loguru import logger

ALIEXPRESS_URL = "https://www.aliexpress.com"
SCROLL_PAUSE_JS = "window.scrollBy({top: window.innerHeight * 0.85, behavior: 'smooth'});"
SCROLL_COUNT = 10

# ── Selectors ────────────────────────────────────────────────────────────────
SEL_CARD = "a._3mPKP"
SEL_TITLE = "h3.yB6en"
SEL_PRICE_CURRENT = "div._23lt5"
SEL_PRICE_ORIGINAL = "div._3DRNh span"
SEL_DISCOUNT = "span.W__kt"
SEL_RATING = "span._2L2Tc"
SEL_SOLD = "span.DUuR2"

_ITEM_ID_RE = re.compile(r"/item/(\d+)\.html")
_NUMBER_RE = re.compile(r"[\d,]+\.?\d*")


# ── Normalisation helpers ─────────────────────────────────────────────────────

def _parse_price(text: str | None) -> float | None:
    if not text:
        return None
    m = _NUMBER_RE.search(text.replace(",", ""))
    return float(m.group()) if m else None


def _parse_sold(text: str | None) -> int | None:
    """'5,000+ sold' → 5000"""
    if not text:
        return None
    m = _NUMBER_RE.search(text.replace(",", ""))
    return int(float(m.group())) if m else None


def _parse_discount(text: str | None) -> int | None:
    """-53% → 53"""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group()) if m else None


def _parse_rating(text: str | None) -> float | None:
    if not text:
        return None
    try:
        return float(text.strip())
    except ValueError:
        return None


def _clean_url(href: str | None) -> str | None:
    if not href:
        return None
    href = href.split("?")[0]
    if href.startswith("//"):
        href = "https:" + href
    return href


def _extract_item_id(href: str | None) -> str | None:
    if not href:
        return None
    m = _ITEM_ID_RE.search(href)
    return m.group(1) if m else None


# ── Parser ────────────────────────────────────────────────────────────────────

def parse_products(html: str) -> list[dict]:
    soup = soupify(html)
    products = []

    for card in soup.select(SEL_CARD):
        href = card.get("href", "")

        title_el = card.select_one(SEL_TITLE)
        title = title_el.get_text(strip=True) if title_el else card.select_one("div._2BLrX") and card.select_one("div._2BLrX").get("title")

        price_el = card.select_one(SEL_PRICE_CURRENT)
        current_price = _parse_price(price_el.get("aria-label") if price_el else None)

        orig_el = card.select_one(SEL_PRICE_ORIGINAL)
        original_price = _parse_price(orig_el.get_text(strip=True) if orig_el else None)

        discount_el = card.select_one(SEL_DISCOUNT)
        discount_pct = _parse_discount(discount_el.get_text(strip=True) if discount_el else None)

        rating_el = card.select_one(SEL_RATING)
        rating = _parse_rating(rating_el.get_text(strip=True) if rating_el else None)

        sold_el = card.select_one(SEL_SOLD)
        sold = _parse_sold(sold_el.get_text(strip=True) if sold_el else None)

        item_id = _extract_item_id(href)
        product_url = _clean_url(href)

        if not item_id or current_price is None:
            continue

        products.append({
            "item_id": item_id,
            "title": title,
            "product_url": product_url,
            "currency": "USD",
            "price": current_price,
            "original_price": original_price,
            "discount_pct": discount_pct,
            "rating": rating,
            "sold_count": sold,
        })

    return products


# ── Browser scraper ───────────────────────────────────────────────────────────

@browser(
    user_agent=UserAgent.RANDOM,
    window_size=WindowSize.RANDOM,
    wait_for_complete_page_load=False,
    remove_default_browser_check_argument=True,
    enable_xvfb_virtual_display=True,
    max_retry=3,
    close_on_crash=True,
    raise_exception=True,
    create_error_logs=False,
)
def scrape_aliexpress_html(driver: Driver, data: dict) -> dict:
    url = data.get("url", ALIEXPRESS_URL)
    scroll_count = data.get("scroll_count", SCROLL_COUNT)

    logger.info(f"[ALIEXPRESS] Navigating → {url}")
    driver.google_get(url, bypass_cloudflare=True)
    driver.long_random_sleep()

    driver.enable_human_mode()
    for i in range(scroll_count):
        logger.info(f"[ALIEXPRESS] Scroll {i + 1}/{scroll_count}")
        driver.run_js(SCROLL_PAUSE_JS)
        driver.short_random_sleep()
    driver.disable_human_mode()

    driver.long_random_sleep()

    html = driver.page_html
    logger.info(f"[ALIEXPRESS] Captured {len(html):,} chars of HTML")

    products = parse_products(html)
    logger.info(f"[ALIEXPRESS] Parsed {len(products)} products")

    return {
        "url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "scroll_count": scroll_count,
        "product_count": len(products),
        "products": products,
    }


# ── Public entry point ────────────────────────────────────────────────────────

def run_aliexpress_scraper(
    url: str = ALIEXPRESS_URL,
    scroll_count: int = SCROLL_COUNT,
) -> dict:
    result = scrape_aliexpress_html({"url": url, "scroll_count": scroll_count})
    return result[0] if isinstance(result, list) else result


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AliExpress product scraper")
    parser.add_argument("--url", default=ALIEXPRESS_URL)
    parser.add_argument("--scrolls", type=int, default=SCROLL_COUNT)
    parser.add_argument("--out", default="aliexpress_products.json")
    args = parser.parse_args()

    payload = run_aliexpress_scraper(url=args.url, scroll_count=args.scrolls)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {payload['product_count']} products → {args.out}")
