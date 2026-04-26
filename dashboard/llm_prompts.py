"""
LLM prompt helpers for financial extraction (Gross Revenue, R&D Expenses).

These strings are intended to be sent to your model layer (OpenAI, Bedrock, etc.)
from a future Streamlit or serverless app.
"""

from __future__ import annotations
import json
import os
import openai

from dashboard.config import (
    DASHBOARD_METRICS_DF_COLUMNS,
    LLM_SYSTEM_PROMPT,
    MODALITY_KEYWORDS,
    build_financial_extraction_system_prompt,
    build_user_prompt_chunk,
)

def call_openai_for_extraction(source_label: str, text: str) -> list[dict]:
    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": get_system_prompt()},
            {"role": "user", "content": get_user_prompt_for_text(source_label, text)},
        ],
    )
    raw = response.choices[0].message.content.strip()
    # Strip markdown code fences if the model wrapped the JSON in ```json ... ```
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"[llm_prompts] Failed to parse JSON for {source_label}")
        return []

def get_system_prompt() -> str:
    return build_financial_extraction_system_prompt()


def get_user_prompt_for_text(source_label: str, text: str) -> str:
    return build_user_prompt_chunk(source_label, text)


def modality_keyword_list() -> list[str]:
    return list(MODALITY_KEYWORDS.values())
