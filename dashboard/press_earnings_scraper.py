"""
Press releases via yfinance news + optional full-article paragraph scrape.

For each ticker: fetch ``ticker.news``, record title / publisher / link, then
(optionally) fetch each link with requests + BeautifulSoup and extract ``<p>`` text.
Writes one aggregate file per ticker: ``raw_transcripts/{ticker}_press_releases.txt``.

Rate limiting: ``time.sleep(3)`` between tickers and between HTTP fetches to reduce
risk of blocks.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from dashboard.config import DEFAULT_PUBLIC_TICKERS, RAW_TRANSCRIPTS_DIR

try:
    import yfinance as yf
except ImportError:
    yf = None  # type: ignore

# SEC / site-friendly pacing (seconds).
RATE_LIMIT_SECONDS: float = 3.0

DEFAULT_NEWS_LIMIT: int = 15
REQUEST_TIMEOUT_SEC: float = 25.0


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _safe_ticker_filename(ticker: str) -> str:
    """Avoid path separators in ticker-based filenames."""
    return ticker.replace("/", "_").replace("\\", "_")


def fetch_yfinance_news_items(
    ticker: str,
    limit: int = DEFAULT_NEWS_LIMIT,
) -> list[dict[str, Any]]:
    """
    Return recent Yahoo Finance news rows with ``title``, ``publisher``, ``link``.

    Extra keys may be present (e.g. providerPublishTime); callers can ignore them.
    """
    if yf is None:
        print(f"[press_earnings] yfinance not installed; skipping news for {ticker}")
        return []

    try:
        t = yf.Ticker(ticker)
        news = getattr(t, "news", None) or []
    except Exception as e:
        print(f"[WARNING] yfinance news failed for {ticker}: {e}")
        return []

    out: list[dict[str, Any]] = []
    for item in news[:limit]:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or item.get("headline") or ""
        link = item.get("link") or item.get("url") or ""
        publisher = (
            item.get("publisher")
            or item.get("publisherName")
            or item.get("source")
            or "N/A"
        )
        out.append(
            {
                "title": title,
                "publisher": publisher,
                "link": link,
                "providerPublishTime": item.get("providerPublishTime"),
            }
        )
    return out


def scrape_press_release_paragraphs_from_url(url: str) -> str | None:
    """
    GET ``url`` and extract visible paragraph text (``<p>`` elements).

    Returns joined plain text, or ``None`` on failure / empty result.
    """
    if not url or not str(url).strip():
        return None

    try:
        headers = {
            "User-Agent": (
                "QiagenMarketResearchDashboard/1.0 "
                "(research; contact: your.email@example.com)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
    except Exception as e:
        print(f"[WARNING] HTTP fetch failed for URL ({url[:80]}...): {e}")
        return None

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        paragraphs: list[str] = []
        for p in soup.find_all("p"):
            text = p.get_text(separator=" ", strip=True)
            if text and len(text) > 1:
                paragraphs.append(text)

        if not paragraphs:
            return None
        return "\n\n".join(paragraphs)
    except Exception as e:
        print(f"[WARNING] BeautifulSoup paragraph extraction failed: {e}")
        return None


def build_press_releases_document(ticker: str, news_limit: int = DEFAULT_NEWS_LIMIT) -> str:
    """
    Assemble one text document: metadata per article + scraped paragraph bodies.
    """
    lines: list[str] = [
        f"ticker: {ticker}",
        f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    try:
        items = fetch_yfinance_news_items(ticker, limit=news_limit)
    except Exception as e:
        return "\n".join(lines + [f"[ERROR] Could not load news for {ticker}: {e}", ""])

    if not items:
        lines.append(f"No news items returned for {ticker}.")
        return "\n".join(lines)

    for idx, item in enumerate(items, start=1):
        title = item.get("title") or ""
        publisher = item.get("publisher") or "N/A"
        link = item.get("link") or ""

        lines.append(f"{'=' * 60}")
        lines.append(f"Article {idx}")
        lines.append(f"Title: {title}")
        lines.append(f"Publisher: {publisher}")
        lines.append(f"Link: {link}")
        lines.append("")

        if link:
            time.sleep(RATE_LIMIT_SECONDS)
            try:
                body = scrape_press_release_paragraphs_from_url(str(link))
            except Exception as e:
                body = None
                print(f"[WARNING] Paragraph scrape raised for {ticker} article {idx}: {e}")

            if body:
                lines.append("--- Paragraph text (scraped) ---")
                # Cap extremely long pages to keep files manageable.
                if len(body) > 400_000:
                    body = body[:400_000] + "\n\n[TRUNCATED]"
                lines.append(body)
            else:
                lines.append("[No paragraph text extracted; link may be paywalled or non-HTML.]")
        else:
            lines.append("[No link available for this item.]")

        lines.append("")

    return "\n".join(lines)


def save_press_releases_file(
    ticker: str,
    content: str,
    project_root: Path | None = None,
) -> Path:
    """Write ``raw_transcripts/{ticker}_press_releases.txt``."""
    root = project_root or _project_root()
    out_dir = root / RAW_TRANSCRIPTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{_safe_ticker_filename(ticker)}_press_releases.txt"
    path = out_dir / fname
    path.write_text(content, encoding="utf-8")
    return path


def fetch_alphavantage_transcripts(ticker: str) -> None:
    """
    Alpha Vantage Earnings Transcript API (requires API key; premium tier).

    TODO: Add API Key — set ``ALPHA_VANTAGE_API_KEY`` in your environment or
    config before enabling. Uncomment and adjust ``function`` / params per
    current Alpha Vantage documentation.

    See: https://www.alphavantage.co/documentation/
    """
    # import os
    #
    # time.sleep(3)
    # api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    # if not api_key:
    #     print("TODO: Add API Key (ALPHA_VANTAGE_API_KEY) for Alpha Vantage transcripts.")
    #     return
    #
    # # Strip exchange suffix if your plan expects base symbol only, e.g. "IBM" not "IBM.TO"
    # symbol = ticker.split(".")[0].upper()
    # url = (
    #     "https://www.alphavantage.co/query"
    #     f"?function=EARNINGS_CALL_TRANSCRIPT&symbol={symbol}&apikey={api_key}"
    # )
    # try:
    #     r = requests.get(url, timeout=30)
    #     r.raise_for_status()
    #     data = r.json()
    #     # Parse and save transcript text to raw_transcripts/ as needed.
    #     print(data)
    # except Exception as e:
    #     print(f"[WARNING] Alpha Vantage transcript request failed: {e}")
    # finally:
    #     time.sleep(3)
    _ = ticker  # silence unused when stub is commented out
    return


def scrape_press_and_news_for_tickers(
    tickers: tuple[str, ...] | list[str] | None = None,
    project_root: Path | None = None,
    sleep_between_tickers: float | None = None,
    news_limit: int = DEFAULT_NEWS_LIMIT,
) -> list[Path]:
    """
    For each ticker: build yfinance news + paragraph scrape document and save
    to ``{ticker}_press_releases.txt``.

    Returns paths of written files (skipped tickers still attempt write with error note).
    """
    root = project_root or _project_root()
    seq = list(tickers) if tickers is not None else list(DEFAULT_PUBLIC_TICKERS)
    pause = sleep_between_tickers if sleep_between_tickers is not None else RATE_LIMIT_SECONDS

    written: list[Path] = []

    for i, ticker in enumerate(seq):
        print(f"[press_earnings] ({i + 1}/{len(seq)}) Building press releases for: {ticker}")
        try:
            doc = build_press_releases_document(ticker, news_limit=news_limit)
            path = save_press_releases_file(ticker, doc, project_root=root)
            written.append(path)
            print(f"  Saved: {path.name}")
        except Exception as e:
            print(f"  [WARNING] Failed to build/save press releases for {ticker}: {e}")
            try:
                err_doc = (
                    f"ticker: {ticker}\n"
                    f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}\n\n"
                    f"[ERROR] Pipeline failed: {e}\n"
                )
                path = save_press_releases_file(ticker, err_doc, project_root=root)
                written.append(path)
            except Exception as e2:
                print(f"  [WARNING] Could not write error stub for {ticker}: {e2}")

        if i < len(seq) - 1:
            time.sleep(pause)

    return written


def run_press_scraper_cli() -> None:
    paths = scrape_press_and_news_for_tickers()
    print(
        f"[press_earnings] Done. Wrote {len(paths)} file(s) under {RAW_TRANSCRIPTS_DIR}/."
    )


if __name__ == "__main__":
    run_press_scraper_cli()
