#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Navigate to the directory of this script (project root).
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Activating virtual environment at venv/bin/activate..."
source venv/bin/activate

echo "Running week2_downloader.py (synchronous downloader)..."
python week2_downloader.py

echo "Running week5_async_scraper.py (concurrent downloader)..."
python week5_async_scraper.py

echo "All scraper scripts have finished."

