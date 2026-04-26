"""
Pandas pipeline: standardized Excel export with citation tracking.

Column schema matches ``dashboard.config.DASHBOARD_METRICS_DF_COLUMNS`` and the
``LLM_SYSTEM_PROMPT`` output contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from dashboard.config import DASHBOARD_METRICS_DF_COLUMNS, EXCEL_OUTPUT_FILENAME, OUTPUTS_DIR
from dashboard.llm_prompts import call_openai_for_extraction

EXCEL_COLUMNS: tuple[str, ...] = DASHBOARD_METRICS_DF_COLUMNS
REVENUE_ROW_LABELS: tuple[str, ...] = ("Total Revenue", "Revenue")
RD_ROW_LABELS: tuple[str, ...] = ("Research And Development", "Research Development")


def _extract_ticker_from_transcript(file_name: str, text: str) -> str | None:
    """Infer ticker from transcript filename or metadata header."""
    for suffix in ("_press_releases.txt", "_10K_clean.txt"):
        if file_name.endswith(suffix):
            return file_name[: -len(suffix)]

    first_line = text.splitlines()[0].strip() if text else ""
    if first_line.lower().startswith("ticker:"):
        ticker = first_line.split(":", 1)[1].strip()
        if ticker:
            return ticker
    return None


def _first_financial_value(financials: Any, labels: tuple[str, ...]) -> float | None:
    """Read the latest non-null value for one of the candidate financial rows."""
    if financials is None or getattr(financials, "empty", True):
        return None
    for label in labels:
        if label in financials.index:
            series = financials.loc[label].dropna()
            if not series.empty:
                return float(series.iloc[0])
    return None


def rows_to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize dict rows to the canonical column schema (missing keys -> None)."""
    normalized: list[dict[str, Any]] = []
    for r in rows:
        normalized.append({col: r.get(col) for col in EXCEL_COLUMNS})
    return pd.DataFrame(normalized, columns=list(EXCEL_COLUMNS))


def export_metrics_excel(
    df: pd.DataFrame,
    output_path: Path | None = None,
    project_root: Path | None = None,
) -> Path:
    """Write DataFrame to ``.xlsx`` (requires openpyxl)."""
    root = project_root or Path(__file__).resolve().parent.parent
    out = output_path or (root / OUTPUTS_DIR / EXCEL_OUTPUT_FILENAME)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="metrics")
    return out

def build_rows_from_llm_extraction(transcripts_dir: Path) -> list[dict]:
    all_rows = []
    for file in sorted(transcripts_dir.iterdir()):
        if not file.is_file():
            continue
        text = file.read_text(encoding="utf-8", errors="ignore")
        ticker = _extract_ticker_from_transcript(file.name, text)
        rows = call_openai_for_extraction(file.name, text)
        if ticker:
            for row in rows:
                row["Ticker"] = ticker
        all_rows.extend(rows)
    return all_rows


def build_rows_from_yfinance(ticker_company_map: dict[str, str]) -> list[dict[str, Any]]:
    """Build structured rows from yfinance info/financials using ticker as key."""
    rows: list[dict[str, Any]] = []
    for ticker, company_name in ticker_company_map.items():
        try:
            tk = yf.Ticker(ticker)
            info = tk.info or {}
            financials = tk.financials
        except Exception as e:
            print(f"[yfinance] Failed to fetch structured data for {ticker}: {e}")
            info = {}
            financials = None

        revenue_local = _first_financial_value(financials, REVENUE_ROW_LABELS)
        rd_expense = _first_financial_value(financials, RD_ROW_LABELS)
        region = info.get("country")
        currency = info.get("currency") or info.get("financialCurrency")
        market_cap = info.get("marketCap")
        total_revenue_info = info.get("totalRevenue")
        revenue_growth = info.get("revenueGrowth")

        citation_parts = [f"yfinance structured ({ticker})"]
        if market_cap is not None:
            citation_parts.append(f"marketCap={market_cap}")
        if total_revenue_info is not None:
            citation_parts.append(f"info.totalRevenue={total_revenue_info}")
        if revenue_growth is not None:
            citation_parts.append(f"revenueGrowth={revenue_growth}")

        rows.append(
            {
                "Ticker": ticker,
                "Company": company_name,
                "APAC Region": region,
                "Modality": None,
                "Reported Currency": currency,
                "Gross Revenue (Local)": revenue_local,
                "Gross Revenue (USD)": revenue_local if str(currency or "").upper() == "USD" else None,
                "R&D Expenses (USD)": rd_expense,
                "Citation": " | ".join(citation_parts),
            }
        )
    return rows


def build_placeholder_rows_from_pdf_extraction(
    venture_pdf_name: str,
    page_range: str,
    ticker_company_name_map: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Seed rows for the Full-Stack / LLM pipeline; modality and financials blank until LLM fills.
    """
    citation = f"{venture_pdf_name} pp.{page_range}"
    rows: list[dict[str, Any]] = []
    for ticker, name in ticker_company_name_map.items():
        rows.append(
            {
                "Ticker": ticker,
                "Company": name,
                "APAC Region": None,
                "Modality": None,
                "Reported Currency": None,
                "Gross Revenue (Local)": None,
                "Gross Revenue (USD)": None,
                "R&D Expenses (USD)": None,
                "Citation": citation,
            }
        )
    return rows
