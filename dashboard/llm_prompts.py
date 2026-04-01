"""
LLM prompt helpers for financial extraction (Gross Revenue, R&D Expenses).

These strings are intended to be sent to your model layer (OpenAI, Bedrock, etc.)
from a future Streamlit or serverless app.
"""

from __future__ import annotations

from dashboard.config import (
    DASHBOARD_METRICS_DF_COLUMNS,
    LLM_SYSTEM_PROMPT,
    MODALITY_KEYWORDS,
    build_financial_extraction_system_prompt,
    build_user_prompt_chunk,
)


def get_system_prompt() -> str:
    return build_financial_extraction_system_prompt()


def get_user_prompt_for_text(source_label: str, text: str) -> str:
    return build_user_prompt_chunk(source_label, text)


def modality_keyword_list() -> list[str]:
    return list(MODALITY_KEYWORDS.values())
