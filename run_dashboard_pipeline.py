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
from dotenv import load_dotenv
load_dotenv()
from dashboard.config import RAW_TRANSCRIPTS_DIR
from dashboard.financial_excel import build_rows_from_llm_extraction

import sys
from pathlib import Path

# Allow running as ``python run_dashboard_pipeline.py`` from repo root.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.config import (
    TICKER_COMPANY_NAME_MAP,
    build_financial_extraction_system_prompt,
)
from dashboard.financial_excel import (
    build_placeholder_rows_from_pdf_extraction,
    export_metrics_excel,
    rows_to_dataframe,
)
from dashboard.press_earnings_scraper import scrape_press_and_news_for_tickers
from dashboard.annual_report_pipeline import clean_sec_filings
from week2_downloader import main as download_sec_filings


def main() -> None:
    print("=== Qiagen Market Research Dashboard pipeline ===\n")

    # 1) Seed company names from ticker map
    names = list(TICKER_COMPANY_NAME_MAP.values())
    print(f"Step 1: Loaded {len(names)} companies from ticker map.\n")

    # 2) Press / news scraper
    try:
        paths = scrape_press_and_news_for_tickers(project_root=ROOT)
        print(f"Step 2: Press/news files written: {len(paths)}\n")
    except Exception as e:
        print(f"Step 2 failed: {e}\n")

    # 3) SEC 10-K downloader
    try:
        print("Step 3: Downloading SEC 10-K filings...")
        download_sec_filings()
        print("Step 3: SEC filings downloaded.\n")
    except Exception as e:
        print(f"Step 3 failed: {e}\n")

    # 4) Clean SEC filings into raw_transcripts/
    try:
        print("Step 4: Cleaning SEC filings into plain text...")
        clean_sec_filings()
        print("Step 4: SEC filings cleaned.\n")
    except Exception as e:
        print(f"Step 4 failed: {e}\n")

    # 5) Excel seed from ticker map + LLM financial data
    placeholder_rows = build_placeholder_rows_from_pdf_extraction(
        venture_pdf_name="config.TICKER_COMPANY_NAME_MAP",
        page_range="all",
        company_names=names,
    )
    llm_rows = build_rows_from_llm_extraction(ROOT / RAW_TRANSCRIPTS_DIR)

    # Index LLM rows by company name for fast lookup
    llm_by_company = {r["Company"].lower(): r for r in llm_rows if r.get("Company")}

    # Merge: fill placeholder fields with LLM data where available
    for row in placeholder_rows:
        match = llm_by_company.get(row["Company"].lower())
        if match:
            for field in ("APAC Region", "Modality", "Reported Currency",
                          "Gross Revenue (Local)", "Gross Revenue (USD)", "R&D Expenses (USD)"):
                if match.get(field) is not None:
                    row[field] = match[field]

    df = rows_to_dataframe(placeholder_rows)
    out_xlsx = export_metrics_excel(df, project_root=ROOT)
    print(f"Step 5: Wrote Excel with LLM data: {out_xlsx}\n")

    # 6) LLM prompt preview
    print("Step 6 — LLM system prompt (preview, first 800 chars):\n")
    sp = build_financial_extraction_system_prompt()
    print(sp[:800] + ("..." if len(sp) > 800 else ""))
    print("\n=== Pipeline finished ===")


if __name__ == "__main__":
    main()
