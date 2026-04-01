#!/usr/bin/env python3
"""
Qiagen Market Research Dashboard — end-to-end data engineering pipeline (CLI).

Steps:
1. Extract company names from Venture Healthcare report PDF (pages 15–20) → master JSON.
2. Seed / append Excel metrics template with citations (PDF source).
3. Scrape press / news text for configured public tickers → ``raw_transcripts/``.
4. Print LLM system prompt preview (Gross Revenue / R&D + modality keywords).

Requires optional deps: yfinance, requests, bs4, pandas, openpyxl, forex-python, spacy (+ model).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running as ``python run_dashboard_pipeline.py`` from repo root.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.company_pdf_extraction import extract_companies_from_venture_report_pages_15_to_20
from dashboard.config import (
    EXCEL_OUTPUT_FILENAME,
    OUTPUTS_DIR,
    VENTURE_HEALTHCARE_REPORT_FILENAME,
    build_financial_extraction_system_prompt,
)
from dashboard.financial_excel import (
    build_placeholder_rows_from_pdf_extraction,
    export_metrics_excel,
    rows_to_dataframe,
)
from dashboard.press_earnings_scraper import scrape_press_and_news_for_tickers


def main() -> None:
    print("=== Qiagen Market Research Dashboard pipeline ===\n")

    # 1) PDF company extraction
    names: list[str] = []
    try:
        names, _pages = extract_companies_from_venture_report_pages_15_to_20(project_root=ROOT)
    except FileNotFoundError as e:
        print(f"Step 1 skipped: {e}\n")
    except Exception as e:
        print(f"Step 1 failed: {e}\n")

    # 2) Excel seed from extracted names (if any)
    if names:
        rows = build_placeholder_rows_from_pdf_extraction(
            venture_pdf_name=VENTURE_HEALTHCARE_REPORT_FILENAME,
            page_range="15-20",
            company_names=names,
        )
        df = rows_to_dataframe(rows)
        out_xlsx = export_metrics_excel(df, project_root=ROOT)
        print(f"Step 2: Wrote Excel template: {out_xlsx}\n")
    else:
        print(
            "Step 2: No companies extracted (PDF missing or empty pages); "
            f"Excel not written. Expected output path: {ROOT / OUTPUTS_DIR / EXCEL_OUTPUT_FILENAME}\n"
        )

    # 3) Press / news scraper
    try:
        paths = scrape_press_and_news_for_tickers(project_root=ROOT)
        print(f"Step 3: Press/news files written: {len(paths)}\n")
    except Exception as e:
        print(f"Step 3 failed: {e}\n")

    # 4) LLM prompt preview
    print("Step 4 — LLM system prompt (preview, first 800 chars):\n")
    sp = build_financial_extraction_system_prompt()
    print(sp[:800] + ("..." if len(sp) > 800 else ""))
    print("\n=== Pipeline finished ===")


if __name__ == "__main__":
    main()
