"""
Pandas pipeline: standardized Excel export with citation tracking.

Column schema matches ``dashboard.config.DASHBOARD_METRICS_DF_COLUMNS`` and the
``LLM_SYSTEM_PROMPT`` output contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.config import DASHBOARD_METRICS_DF_COLUMNS, EXCEL_OUTPUT_FILENAME, OUTPUTS_DIR
from dashboard.llm_prompts import call_openai_for_extraction

EXCEL_COLUMNS: tuple[str, ...] = DASHBOARD_METRICS_DF_COLUMNS


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
        rows = call_openai_for_extraction(file.name, text)
        all_rows.extend(rows)
    return all_rows

def build_placeholder_rows_from_pdf_extraction(
    venture_pdf_name: str,
    page_range: str,
    company_names: list[str],
) -> list[dict[str, Any]]:
    """
    Seed rows for the Full-Stack / LLM pipeline; modality and financials blank until LLM fills.
    """
    citation = f"{venture_pdf_name} pp.{page_range}"
    rows: list[dict[str, Any]] = []
    for name in company_names:
        rows.append(
            {
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
