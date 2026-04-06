from pathlib import Path
from dashboard.config import DEFAULT_PUBLIC_TICKERS
import time

from sec_edgar_downloader import Downloader


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


def main() -> None:
    # Strictly US-listed healthcare tickers that file 10-Ks.
    tickers = [t for t in DEFAULT_PUBLIC_TICKERS if not t.endswith(('.T', '.SW', '.AX', '.KQ', '.SZ', '.SS', '.OL', '.DE'))]

    output_dir = ensure_output_dir("sec-edgar-filings")
    download_latest_10k(tickers, output_dir)

if __name__ == "__main__":
    main()