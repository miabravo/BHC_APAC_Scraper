"""
Concurrent SEC 10-K downloader (ThreadPoolExecutor).

Downloads each ticker's most recent 10-K filing into `raw_pdfs/` using
`sec-edgar-downloader`.
"""

from __future__ import annotations

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable
import time

from sec_edgar_downloader import Downloader


def download_latest_10k_for_ticker(
    ticker: str,
    output_dir: Path,
    company_name: str,
    contact_email: str,
) -> tuple[str, bool, str]:
    """
    Worker function for downloading the most recent 10-K.

    Returns: (ticker, success, message)
    """
    try:
        dl = Downloader(company_name, contact_email, str(output_dir))
        # Updated API: use `limit=1` to fetch the most recent filing.
        dl.get("10-K", ticker, limit=1)
        return ticker, True, "Downloaded latest 10-K"
    except Exception as e:
        return ticker, False, f"Download failed: {e}"


def download_latest_10ks_concurrently(
    tickers: Iterable[str],
    output_dir: Path,
    company_name: str,
    contact_email: str,
    max_workers: int = 3,
) -> None:
    """
    Download latest 10-K filings concurrently using up to `max_workers` threads.
    """
    tickers_list = list(tickers)

    # ThreadPoolExecutor is used to parallelize network I/O.
    # Keep workers low (default 3) to reduce risk of SEC rate-limiting/IP blocks.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker: dict[concurrent.futures.Future, str] = {}

        for ticker in tickers_list:
            print(f"Queueing ticker: {ticker}")
            fut = executor.submit(
                download_latest_10k_for_ticker,
                ticker,
                output_dir,
                company_name,
                contact_email,
            )
            future_to_ticker[fut] = ticker

        # Print results as each ticker finishes.
        for fut in as_completed(future_to_ticker):
            ticker = future_to_ticker[fut]
            try:
                downloaded_ticker, ok, message = fut.result()
                # In practice, downloaded_ticker should equal `ticker`.
                if ok:
                    print(f"[SUCCESS] {downloaded_ticker}: {message}")
                else:
                    print(f"[FAIL]    {downloaded_ticker}: {message}")
            except Exception as e:
                # Extra safeguard: should be rare because the worker catches errors.
                print(f"[FAIL]    {ticker}: Worker raised an exception: {e}")

            # Small pacing to avoid bursts in the print/IO loop.
            time.sleep(0.2)


def main() -> None:
    # Strictly US-listed healthcare tickers that file 10-Ks.
    tickers = ["TMO", "BIO", "ILMN", "VRTX"]

    output_dir = Path("raw_pdfs")
    output_dir.mkdir(parents=True, exist_ok=True)

    # SEC-friendly User-Agent (built internally by the library).
    company_name = "QiagenScraperProject"
    contact_email = "your.email@example.com"  # TODO: replace with your real email

    download_latest_10ks_concurrently(
        tickers=tickers,
        output_dir=output_dir,
        company_name=company_name,
        contact_email=contact_email,
        max_workers=3,
    )

    print(f"Done. Check downloads under: {output_dir.resolve()}")


if __name__ == "__main__":
    main()

