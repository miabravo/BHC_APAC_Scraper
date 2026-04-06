"""
Press releases via Google News RSS + newspaper3k full-article extraction.
Bypasses the broken yfinance API entirely.
"""

import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from newspaper import Article

from dashboard.config import DEFAULT_PUBLIC_TICKERS, RAW_TRANSCRIPTS_DIR

# SEC / site-friendly pacing (seconds).
RATE_LIMIT_SECONDS: float = 3.0
DEFAULT_NEWS_LIMIT: int = 5
REQUEST_TIMEOUT_SEC: float = 25.0

def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent

def _safe_ticker_filename(ticker: str) -> str:
    """Avoid path separators in ticker-based filenames."""
    return ticker.replace("/", "_").replace("\\", "_")

def fetch_google_news_rss(ticker: str, limit: int = DEFAULT_NEWS_LIMIT) -> list[dict]:
    """
    Fetch the top news items for a ticker using Google News RSS.
    """
    # Using the ticker + financial keywords to filter out noise
    query = f'"{ticker}" AND (revenue OR APAC OR expansion)'
    encoded_query = urllib.parse.quote(query)
    url = f'https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en'
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')[:limit]
        
        results = []
        for item in items:
            title = item.title.text if item.title else "No Title"
            link = item.link.text if item.link else ""
            pubDate = item.pubDate.text if item.pubDate else "No Date"
            results.append({
                "title": title,
                "link": link,
                "pubDate": pubDate
            })
        return results
    except Exception as e:
        print(f"[WARNING] Google News RSS failed for {ticker}: {e}")
        return []

def build_press_releases_document(ticker: str, news_limit: int = DEFAULT_NEWS_LIMIT) -> str:
    """
    Assemble one text document: metadata per article + scraped body text.
    """
    lines = [
        f"ticker: {ticker}",
        f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        ""
    ]

    items = fetch_google_news_rss(ticker, limit=news_limit)

    if not items:
        lines.append(f"No news items returned for {ticker}.")
        return "\n".join(lines)

    for idx, item in enumerate(items, start=1):
        lines.append(f"{'=' * 60}")
        lines.append(f"Article {idx} of {len(items)}")
        lines.append(f"Title: {item['title']}")
        lines.append(f"Date: {item['pubDate']}")
        lines.append(f"Link: {item['link']}")
        lines.append("")

        if item['link']:
            time.sleep(2) # Give the target server a breather
            try:
                article = Article(item['link'])
                article.download()
                article.parse()
                text = article.text.strip()
                
                if text:
                    lines.append("--- Paragraph text (scraped) ---")
                    lines.append(text)
                else:
                    lines.append("[Body text blocked by paywall/JS. AI: Please infer market context from the Title and Date.]")
            except Exception as e:
                lines.append("[Body text blocked by paywall/JS. AI: Please infer market context from the Title and Date.]")
        else:
            lines.append("[No link available for this item.]")

        lines.append("")

    return "\n".join(lines)

def save_press_releases_file(ticker: str, content: str, project_root: Path | None = None) -> Path:
    """Write ``raw_transcripts/{ticker}_press_releases.txt``."""
    root = project_root or _project_root()
    out_dir = root / RAW_TRANSCRIPTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"{_safe_ticker_filename(ticker)}_press_releases.txt"
    path = out_dir / fname
    path.write_text(content, encoding="utf-8")
    return path

def scrape_press_and_news_for_tickers(
    tickers: list[str] | None = None,
    project_root: Path | None = None,
    sleep_between_tickers: float | None = None,
    news_limit: int = DEFAULT_NEWS_LIMIT,
) -> list[Path]:
    
    root = project_root or _project_root()
    seq = list(tickers) if tickers is not None else list(DEFAULT_PUBLIC_TICKERS)
    pause = sleep_between_tickers if sleep_between_tickers is not None else RATE_LIMIT_SECONDS

    written: list[Path] = []

    for i, ticker in enumerate(seq):
        print(f"[press_earnings] ({i + 1}/{len(seq)}) Fetching Google News for: {ticker}")
        try:
            doc = build_press_releases_document(ticker, news_limit=news_limit)
            path = save_press_releases_file(ticker, doc, project_root=root)
            written.append(path)
            print(f"  Saved: {path.name}")
        except Exception as e:
            print(f"  [WARNING] Failed pipeline for {ticker}: {e}")

        if i < len(seq) - 1:
            time.sleep(pause)

    return written

if __name__ == "__main__":
    paths = scrape_press_and_news_for_tickers()
    print(f"[press_earnings] Done. Wrote {len(paths)} file(s) under {RAW_TRANSCRIPTS_DIR}/.")


    