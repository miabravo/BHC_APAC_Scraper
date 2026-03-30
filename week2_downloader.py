from pathlib import Path
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

    for ticker in tickers:
        try:
            print(f"Downloading latest 10-K for {ticker}...")
            # `limit=1` (or `amount=1` depending on library version) gets the most recent filing
            dl.get("10-K", ticker, limit=1)
        except Exception as e:
            try:
                # Fallback for older versions that use `amount` instead of `limit`
                dl.get("10-K", ticker, amount=1)
            except Exception as e_fallback:
                print(f"  Warning: failed to download 10-K for {ticker}: {e_fallback}")
                # Move on to the next ticker without crashing the whole script
                continue
        finally:
            # Be a good API citizen and respect rate limits between requests
            time.sleep(3)

    print(f"\nDownloads (if any) saved under: {output_dir.resolve()}")


def main() -> None:
    # Example US-listed tickers
    tickers = ["TMO", "BIO", "QGEN"]

    output_dir = ensure_output_dir("raw_pdfs")
    download_latest_10k(tickers, output_dir)


if __name__ == "__main__":
    main()
    
from sec_edgar_downloader import Downloader
import os

# 1. Set up the downloader
# The SEC requires your company name and email to download data.
dl = Downloader("BruinHealthConsulting", "your.email@example.com")

# 2. Define the target company and folder
ticker = "TMO" # Thermo Fisher
print(f"Starting download for {ticker} 10-K (Annual Report)...")

try:
    # 3. Download the most recent (1) 10-K filing
    # This will automatically create a folder called 'sec-edgar-filings' in your project
    dl.get("10-K", ticker, limit=1)
    
    print(f"✅ Success! Check the left-hand menu in Cursor.")
    print(f"You should see a new folder containing the massive 10-K document for {ticker}.")

except Exception as e:
    print(f"❌ An error occurred: {e}")


