from pathlib import Path
import time
from datetime import datetime, timezone

import yfinance as yf

from dashboard.config import DEFAULT_PUBLIC_TICKERS, RAW_TRANSCRIPTS_DIR

from sec_edgar_downloader import Downloader

APAC_TICKER_SUFFIXES = (".T", ".KQ", ".AX", ".SS", ".SZ", ".SW", ".OL", ".DE")


def ensure_output_dir(dir_name: str) -> Path:
    """
    Ensure the output directory exists and return its Path.
    """
    output_path = Path(dir_name)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def download_latest_10k(tickers: list[str], output_dir: Path) -> None:
    """
    Download the most recent 10-K filing for each given ticker symbol into
    the specified output directory.
    """
    # Replace with your real details before heavy use
    company_name = "MyResearchProject"
    contact_email = "your.email@example.com"

    # `Downloader` will construct a SEC-compliant User-Agent from these fields.
    try:
        dl = Downloader(company_name, contact_email, str(output_dir))
    except Exception as e:
        raise RuntimeError(f"Failed to initialize SEC EDGAR Downloader: {e}") from e

    for idx, ticker in enumerate(tickers, start=1):
        print(f"[{idx}/{len(tickers)}] Downloading latest 10-K for {ticker}...")
        try:
            # `limit=1` gets the most recent filing under the updated API.
            dl.get("10-K", ticker, limit=1)
            print(f"  Success: downloaded latest 10-K for {ticker}.")
        except Exception as e:
            print(
                f"  Warning: failed to download latest 10-K for {ticker}: {e}"
            )

        # Be a good API citizen and respect rate limits between requests
        time.sleep(3)

    print(f"\nDownloads (if any) saved under: {output_dir.resolve()}")


def _safe_ticker_filename(ticker: str) -> str:
    return ticker.replace("/", "_").replace("\\", "_")


def _build_yfinance_news_document(ticker: str, news_items: list[dict]) -> str:
    lines: list[str] = [
        f"ticker: {ticker}",
        f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "source: yfinance_news_feed",
        "",
    ]
    if not news_items:
        lines.append(f"No yfinance news items returned for {ticker}.")
        return "\n".join(lines)

    for idx, item in enumerate(news_items, start=1):
        title = str(item.get("title") or "No Title")
        summary = str(item.get("summary") or item.get("content") or "").strip()
        publisher = str(item.get("publisher") or "Unknown Publisher")
        link = str(item.get("link") or item.get("url") or "")
        publish_time = item.get("providerPublishTime")

        lines.append("=" * 60)
        lines.append(f"Article {idx} of {len(news_items)}")
        lines.append(f"Title: {title}")
        lines.append(f"Publisher: {publisher}")
        if publish_time:
            lines.append(f"Publish Time (epoch): {publish_time}")
        if link:
            lines.append(f"Link: {link}")
        lines.append("")
        if summary:
            lines.append("--- Summary / Body ---")
            lines.append(summary)
        else:
            lines.append("[No summary text available in yfinance feed.]")
        lines.append("")

    return "\n".join(lines)


def write_apac_yfinance_news_fallback(
    tickers: list[str],
    transcripts_dir: Path,
    news_limit: int = 8,
) -> None:
    """Write yfinance news fallback docs for APAC tickers into raw_transcripts/."""
    transcripts_dir.mkdir(parents=True, exist_ok=True)

    for idx, ticker in enumerate(tickers, start=1):
        print(f"[fallback {idx}/{len(tickers)}] Fetching yfinance news for {ticker}...")
        news_items: list[dict] = []
        try:
            info = yf.Ticker(ticker)
            raw_news = info.news or []
            news_items = raw_news[:news_limit]
        except Exception as e:
            print(f"  Warning: yfinance news fetch failed for {ticker}: {e}")

        content = _build_yfinance_news_document(ticker, news_items)
        out_path = transcripts_dir / f"{_safe_ticker_filename(ticker)}_yf_news.txt"
        out_path.write_text(content, encoding="utf-8")
        print(f"  Saved fallback transcript: {out_path.name}")
        time.sleep(1.5)


def main() -> None:
    # US-listed healthcare tickers that file 10-Ks.
    us_tickers = [t for t in DEFAULT_PUBLIC_TICKERS if not t.endswith(APAC_TICKER_SUFFIXES)]
    apac_tickers = [t for t in DEFAULT_PUBLIC_TICKERS if t.endswith(APAC_TICKER_SUFFIXES)]

    output_dir = ensure_output_dir("sec-edgar-filings")
    download_latest_10k(us_tickers, output_dir)

    # APAC tickers do not file U.S. 10-Ks, so generate text fallback from yfinance news.
    transcripts_dir = ensure_output_dir(RAW_TRANSCRIPTS_DIR)
    write_apac_yfinance_news_fallback(apac_tickers, transcripts_dir)

if __name__ == "__main__":
    main()