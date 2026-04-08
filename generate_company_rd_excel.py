#!/usr/bin/env python3
"""
Load press-release transcripts and SEC-style filings from ``raw_transcripts`` and
``raw_pdfs`` (including nested folders), then call the OpenAI API to produce one
spreadsheet row per company with R&D / revenue breakdowns.

By default **batched** requests are used, split by company count and payload size so
large inputs stay within context limits. ``429`` / rate-limit responses are retried
with backoff (see ``--max-retries``). Use ``--per-company`` for one API call per ticker.

Requires ``OPENAI_API_KEY``. Optional: ``OPENAI_MODEL`` (default: gpt-4o-mini),
``OPENAI_BASE_URL`` for compatible proxies.

Monetary columns are **thousands of USD** (e.g. ``5000`` means five million dollars). The model must **fill
every cell** with **best‑effort estimates** when ``raw_transcripts`` / ``raw_pdfs`` lack line
items—avoid rows of all zeros unless the business is clearly unrelated to that modality.
Responses use OpenAI **structured outputs** (Pydantic + ``parse``). Use a model that supports
structured outputs (e.g. ``gpt-4o-mini``, ``gpt-4o``).

Each successful API call can **append** system prompt, user prompt, and parsed JSON response to
``llm_conversation_log.txt`` (override with ``--conversation-log``; disable with ``--no-conversation-log``).
"""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Type, TypeVar

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

DEFAULT_TRANSCRIPTS = Path("raw_transcripts")
DEFAULT_PDFS = Path("raw_pdfs")
DEFAULT_OUTPUT = Path("company_rd_summary.xlsx")
DEFAULT_CONVERSATION_LOG = Path("llm_conversation_log.txt")

EXCEL_COLUMNS = [
    "Company name",
    "R&d focus",
    "AAV/LV r&d (USD thousands)",
    "AAV/LV revenue (USD thousands)",
    "gene therapy r&d (USD thousands)",
    "gene therapy revenue (USD thousands)",
    "mesenchymal stem cell (MSC) r&d (USD thousands)",
    "MSC revenue (USD thousands)",
]

KEYWORDS_SEC = [
    "research and development",
    "r&d",
    "gene therapy",
    "gene",
    "aav",
    "adeno-associated",
    "lentiviral",
    "lenti",
    "vector",
    "mesenchymal",
    "stem cell",
    "msc",
    "item 1",
    "item 7",
    "business",
    "md&a",
    "revenue",
    "net sales",
]

# ---------------------------------------------------------------------------
# Region inference (for peer-based estimates)
# ---------------------------------------------------------------------------

REGION_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\.(SS|SZ|BJ)$"), "Mainland China listed"),
    (re.compile(r"\.(KQ|KS)$"), "South Korea listed"),
    (re.compile(r"\.T$"), "Japan listed"),
    (re.compile(r"\.AX$"), "Australia listed"),
    (re.compile(r"\.(HK|HI)$"), "Hong Kong listed"),
    (re.compile(r"\.DE$"), "Germany listed"),
    (re.compile(r"\.OL$"), "Oslo listed"),
    (re.compile(r"\.SW$"), "Switzerland listed"),
]


def infer_region(ticker: str) -> str:
    t = ticker.upper()
    for pat, label in REGION_RULES:
        if pat.search(t):
            return label
    if re.fullmatch(r"[A-Z]{1,5}", t):
        return "US / generic equity ticker (assume US unless text implies otherwise)"
    return "Other / unknown"


# ---------------------------------------------------------------------------
# File reading
# ---------------------------------------------------------------------------


def read_text_file(path: Path, max_bytes: int = 30_000_000) -> str:
    data = path.read_bytes()[:max_bytes]
    return data.decode("utf-8", errors="replace")


# Avoid multi-minute BeautifulSoup runs on full EDGAR submissions.
MAX_CHARS_BEFORE_STRIP = 6_000_000


def strip_html_to_text(html_or_mixed: str) -> str:
    soup = BeautifulSoup(html_or_mixed, "html.parser")
    return soup.get_text(separator="\n")


def read_pdf_text(path: Path, max_pages: int = 80) -> str:
    parts: list[str] = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages[:max_pages]):
            t = page.extract_text()
            if t:
                parts.append(t)
    return "\n\n".join(parts)


def read_any_source_file(path: Path) -> str:
    suf = path.suffix.lower()
    if suf == ".pdf":
        return read_pdf_text(path)
    raw = read_text_file(path)
    if len(raw) > MAX_CHARS_BEFORE_STRIP:
        raw = raw[:MAX_CHARS_BEFORE_STRIP] + "\n\n[... truncated before HTML strip ...]\n"
    head = raw[:8000].lower()
    if suf in {".htm", ".html"} or "<html" in head or "<document>" in head:
        return strip_html_to_text(raw)
    if "<sec-document" in head or "<text>" in head or "<type>10-k" in head:
        return strip_html_to_text(raw)
    return raw


def parse_company_name_from_sec_header(raw: str) -> str | None:
    m = re.search(r"COMPANY CONFORMED NAME:\s*([^\t\r\n]+)", raw)
    if m:
        return m.group(1).strip()
    return None


def extract_press_titles(transcript: str) -> str:
    lines = []
    for line in transcript.splitlines():
        s = line.strip()
        if s.startswith("Title:"):
            lines.append(s)
    return "\n".join(lines[:40]) if lines else transcript[:4000]


def extract_keyword_windows(text: str, max_chars: int) -> str:
    """Prefer passages around R&D / therapy keywords; fall back to head+tail."""
    if len(text) <= max_chars:
        return text
    lower = text.lower()
    spans: list[tuple[int, int]] = []
    for kw in KEYWORDS_SEC:
        start = 0
        while True:
            idx = lower.find(kw, start)
            if idx == -1:
                break
            lo = max(0, idx - 1200)
            hi = min(len(text), idx + len(kw) + 1800)
            spans.append((lo, hi))
            start = idx + 1
    if not spans:
        head = text[: max_chars // 2]
        tail = text[-(max_chars // 2) :]
        return head + "\n\n[... middle omitted ...]\n\n" + tail
    spans.sort()
    merged: list[tuple[int, int]] = []
    for lo, hi in spans:
        if not merged or lo > merged[-1][1] + 50:
            merged.append((lo, hi))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
    out_parts: list[str] = []
    used = 0
    for lo, hi in merged:
        chunk = text[lo:hi]
        if used + len(chunk) > max_chars:
            chunk = chunk[: max_chars - used]
        out_parts.append(chunk)
        used += len(chunk)
        if used >= max_chars:
            break
    return "\n\n--- excerpt ---\n\n".join(out_parts)


def ticker_from_transcript_path(path: Path) -> str | None:
    name = path.name
    if not name.endswith("_press_releases.txt"):
        return None
    return name[: -len("_press_releases.txt")]


def ticker_from_sec_path(path: Path) -> str | None:
    parts = path.parts
    for i, p in enumerate(parts):
        if p == "sec-edgar-filings" and i + 1 < len(parts):
            return parts[i + 1]
    return None


# SEC / scraped text often contains NULs or C0 controls that break JSON request bodies.
_CTRL_EXCEPT_WHITESPACE_RE = re.compile(r"[\x00\x01-\x08\x0b\x0c\x0e-\x1f\x7f]")


def sanitize_text_for_api(s: str) -> str:
    """Strip bytes/controls that can make the OpenAI HTTP JSON body invalid."""
    if not s:
        return s
    s = s.replace("\u2028", "\n").replace("\u2029", "\n")
    s = _CTRL_EXCEPT_WHITESPACE_RE.sub(" ", s)
    return s


@dataclass
class CompanyBundle:
    ticker: str
    transcript_paths: list[Path] = field(default_factory=list)
    sec_paths: list[Path] = field(default_factory=list)
    transcript_text: str = ""
    sec_text: str = ""
    company_name_hint: str | None = None


def discover_companies(
    transcripts_dir: Path,
    pdfs_dir: Path,
) -> dict[str, CompanyBundle]:
    bundles: dict[str, CompanyBundle] = {}

    if transcripts_dir.is_dir():
        for path in transcripts_dir.rglob("*"):
            if not path.is_file():
                continue
            t = ticker_from_transcript_path(path)
            if not t:
                continue
            b = bundles.setdefault(t, CompanyBundle(ticker=t))
            b.transcript_paths.append(path)

    if pdfs_dir.is_dir():
        for path in pdfs_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".txt", ".htm", ".html", ".pdf"}:
                continue
            t = ticker_from_sec_path(path)
            if not t:
                continue
            b = bundles.setdefault(t, CompanyBundle(ticker=t))
            b.sec_paths.append(path)

    for t, b in bundles.items():
        tr_parts: list[str] = []
        for p in sorted(set(b.transcript_paths)):
            tr_parts.append(f"=== FILE: {p} ===\n" + read_text_file(p))
        b.transcript_text = sanitize_text_for_api("\n\n".join(tr_parts))
        if b.transcript_text:
            m = re.search(r"^ticker:\s*(\S+)", b.transcript_text, re.MULTILINE)
            if m and m.group(1).upper() == t.upper():
                pass
        sec_parts: list[str] = []
        name_from_sec: str | None = None
        for p in sorted(set(b.sec_paths)):
            raw = read_any_source_file(p)
            if name_from_sec is None:
                name_from_sec = parse_company_name_from_sec_header(raw)
            body = extract_keyword_windows(raw, max_chars=220_000)
            sec_parts.append(f"=== FILE: {p} ===\n" + body)
        b.sec_text = sanitize_text_for_api("\n\n".join(sec_parts))
        b.company_name_hint = name_from_sec
    return bundles


# ---------------------------------------------------------------------------
# OpenAI — structured outputs (all fields required; estimates allowed)
# ---------------------------------------------------------------------------

TModel = TypeVar("TModel", bound=BaseModel)

DEFAULT_BATCH_MAX_CHARS = 400_000
DEFAULT_MAX_COMPANIES_PER_BATCH = 5
DEFAULT_INTER_BATCH_SLEEP_SEC = 8.0
DEFAULT_OPENAI_MAX_RETRIES = 8

POLICY_FILL_ALL = (
    "OUTPUT CONTRACT (mandatory):\n"
    "- Every field in the schema must be populated. Do not use null, omit keys, or leave strings blank.\n"
    "- All monetary fields are float values in **thousands of USD** (1 unit = $1,000). Example: $12.5M R&D → 12500. "
    "If your source states millions, multiply by 1000; if it states dollars, divide by 1000. Non‑negative only.\n"
    "- When ``raw_pdfs`` / ``raw_transcripts`` do **not** disclose modality‑specific R&D or revenue, you must still output "
    "**best estimates**: infer from total R&D and total revenue (MD&A, income statement), implied program mix in Item 1 / "
    "pipeline language, press headlines, and **regional peers** in the same batch (or peer section). Calibrate to company "
    "scale (revenue, headcount hints, filing size). **Do not** default to all zeros—a row of zeros across modalities is "
    "wrong unless the company is clearly a non‑cell‑therapy / non‑vector tools business with no plausible exposure.\n"
    "- Use **0.0** in a monetary field only when the narrative clearly excludes that modality; state that in rd_focus. "
    "Otherwise use a small but non‑zero implied allocation when any therapeutic R&D exists.\n"
    "- company_name: use SEC header, transcript, or best known legal name—never a placeholder like 'unknown'.\n"
    "- rd_focus: 2–5 sentences on R&D themes, modalities, and **how** you estimated or converted figures (including peer "
    "benchmarks) when sources are thin.\n"
    "Try your absolute best to make a guess for each of the fields. Just give the most likely number, based on what you see fit.\n"
)


class SingleCompanyRow(BaseModel):
    """One Excel row; all fields required for predictable parsing."""

    model_config = ConfigDict(extra="forbid")

    company_name: str = Field(
        ...,
        min_length=1,
        description="Legal or common name from SEC/transcript; never empty.",
    )
    rd_focus: str = Field(
        ...,
        min_length=1,
        description="2–5 sentences: R&D themes, modalities, how numbers were estimated if needed.",
    )
    aav_lv_rd: float = Field(
        ...,
        ge=0,
        description="AAV/lentiviral R&D, thousands USD; best estimate if not in source.",
    )
    aav_lv_revenue: float = Field(
        ...,
        ge=0,
        description="AAV/LV revenue, thousands USD; best estimate if not in source.",
    )
    gene_therapy_rd: float = Field(
        ...,
        ge=0,
        description="Gene therapy R&D, thousands USD; best estimate if not in source.",
    )
    gene_therapy_revenue: float = Field(
        ...,
        ge=0,
        description="Gene therapy revenue, thousands USD; best estimate if not in source.",
    )
    msc_rd: float = Field(
        ...,
        ge=0,
        description="MSC R&D, thousands USD; best estimate if not in source.",
    )
    msc_revenue: float = Field(
        ...,
        ge=0,
        description="MSC revenue, thousands USD; best estimate if not in source.",
    )


class BatchCompanyRow(SingleCompanyRow):
    ticker: str = Field(
        ...,
        min_length=1,
        description="Must match the COMPANY TICKER line for that section.",
    )


class BatchRowsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows: list[BatchCompanyRow] = Field(
        ...,
        description="Exactly one entry per required ticker in the user message.",
    )


_RE_RETRY_SECONDS = re.compile(
    r"Please retry in\s+([\d.]+)\s*s",
    re.IGNORECASE,
)


def is_rate_limit_error(err: BaseException) -> bool:
    name = type(err).__name__
    if "RateLimit" in name:
        return True
    s = str(err).lower()
    return (
        "429" in s
        or "rate limit" in s
        or "too many requests" in s
        or ("quota" in s and ("exceeded" in s or "limit" in s))
    )


def retry_delay_seconds_from_error(err: BaseException) -> float | None:
    m = _RE_RETRY_SECONDS.search(str(err))
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    ra = getattr(err, "response", None)
    if ra is not None:
        h = getattr(ra, "headers", None)
        if h:
            v = h.get("retry-after") or h.get("Retry-After")
            if v:
                try:
                    return float(v)
                except ValueError:
                    pass
    return None


def append_llm_conversation_log(
    path: Path,
    *,
    model: str,
    heading: str,
    system: str,
    user: str,
    response_json: str,
) -> None:
    """Append one readable record (prompts + response) to a text file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(
            "# LLM prompt/response transcript — generate_company_rd_excel.py\n"
            "# Each block: UTC timestamp, heading, model id, then SYSTEM / USER / RESPONSE.\n\n",
            encoding="utf-8",
        )
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sep = "=" * 88
    block = (
        f"\n{sep}\n"
        f"{ts}\n"
        f"{heading}\n"
        f"model: {model}\n"
        f"{sep}\n\n"
        f"### SYSTEM\n\n{system}\n\n"
        f"### USER\n\n{user}\n\n"
        f"### RESPONSE (parsed JSON)\n\n{response_json}\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(block)


def openai_parse_with_retry(
    client: OpenAI,
    model: str,
    system: str,
    user: str,
    response_format: Type[TModel],
    *,
    max_retries: int,
    log_prefix: str = "",
    conversation_log: Path | None = None,
    log_heading: str = "",
) -> TModel:
    """Structured completion via ``beta.chat.completions.parse``; retries on rate limits."""
    system = sanitize_text_for_api(system)
    user = sanitize_text_for_api(user)
    for attempt in range(max_retries):
        try:
            completion = client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.2,
                response_format=response_format,
            )
            msg = completion.choices[0].message
            if getattr(msg, "refusal", None):
                raise RuntimeError(f"model refusal: {msg.refusal}")
            parsed = msg.parsed
            if parsed is None:
                raise ValueError("missing structured output (parsed is None)")
            if conversation_log is not None:
                heading = log_heading or log_prefix or "OpenAI chat.completions.parse"
                append_llm_conversation_log(
                    conversation_log,
                    model=model,
                    heading=heading,
                    system=system,
                    user=user,
                    response_json=parsed.model_dump_json(indent=2),
                )
            return parsed
        except Exception as e:
            if not is_rate_limit_error(e):
                raise
            if attempt >= max_retries - 1:
                raise
            suggested = retry_delay_seconds_from_error(e)
            backoff = 25.0 * (1.4**attempt)
            wait = max(suggested or 0.0, backoff) + 2.0
            wait = min(wait, 180.0)
            tag = f"[{log_prefix}] " if log_prefix else ""
            print(
                f"{tag}OpenAI rate limit; waiting {wait:.1f}s "
                f"(retry {attempt + 2}/{max_retries})…"
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def build_peer_context(
    bundles: dict[str, CompanyBundle],
    focus_ticker: str,
) -> str:
    region = infer_region(focus_ticker)
    lines: list[str] = [f"Inferred region for {focus_ticker}: {region}", ""]
    for t, b in sorted(bundles.items()):
        if t == focus_ticker:
            continue
        if region != "Other / unknown" and infer_region(t) != region:
            continue
        titles = extract_press_titles(b.transcript_text) if b.transcript_text else ""
        hint = b.company_name_hint or ""
        lines.append(f"- Ticker {t} (name hint: {hint})")
        if titles:
            lines.append("  Headlines / titles:")
            for ln in titles.splitlines()[:12]:
                lines.append("  " + ln[:500])
        lines.append("")
    return "\n".join(lines) if len(lines) > 4 else "(No peer tickers in this batch.)"


def parsed_row_to_excel(row: SingleCompanyRow) -> dict[str, Any]:
    """Deterministic map from validated model → Excel columns."""
    return {
        "Company name": row.company_name,
        "R&d focus": row.rd_focus,
        "AAV/LV r&d (USD thousands)": row.aav_lv_rd,
        "AAV/LV revenue (USD thousands)": row.aav_lv_revenue,
        "gene therapy r&d (USD thousands)": row.gene_therapy_rd,
        "gene therapy revenue (USD thousands)": row.gene_therapy_revenue,
        "mesenchymal stem cell (MSC) r&d (USD thousands)": row.msc_rd,
        "MSC revenue (USD thousands)": row.msc_revenue,
    }


def error_excel_row(ticker: str, bundle: CompanyBundle | None, message: str) -> dict[str, Any]:
    """Fallback when the API fails; dollar columns left blank (no placeholder zeros)."""
    nan = float("nan")
    hint = bundle.company_name_hint if bundle else None
    return {
        "Company name": (hint or ticker or "Unknown company").strip() or "Unknown company",
        "R&d focus": message,
        "AAV/LV r&d (USD thousands)": nan,
        "AAV/LV revenue (USD thousands)": nan,
        "gene therapy r&d (USD thousands)": nan,
        "gene therapy revenue (USD thousands)": nan,
        "mesenchymal stem cell (MSC) r&d (USD thousands)": nan,
        "MSC revenue (USD thousands)": nan,
    }


def estimate_bundle_chars(bundle: CompanyBundle) -> int:
    return len(bundle.transcript_text) + len(bundle.sec_text) + 800


def chunk_bundles(
    bundles: dict[str, CompanyBundle],
    max_chars: int,
    max_companies: int,
) -> list[dict[str, CompanyBundle]]:
    """Split by character budget and max companies per request."""
    items = sorted(bundles.items(), key=lambda x: x[0])
    chunks: list[dict[str, CompanyBundle]] = []
    current: dict[str, CompanyBundle] = {}
    current_size = 0
    for ticker, bundle in items:
        need = estimate_bundle_chars(bundle)
        should_split = current and (
            len(current) >= max_companies or current_size + need > max_chars
        )
        if should_split:
            chunks.append(current)
            current = {}
            current_size = 0
        current[ticker] = bundle
        current_size += need
    if current:
        chunks.append(current)
    return chunks


def build_batch_document(chunk: dict[str, CompanyBundle]) -> str:
    parts: list[str] = []
    for ticker, bundle in sorted(chunk.items()):
        parts.append(
            f"\n{'=' * 72}\nCOMPANY TICKER: {ticker}\n"
            f"Inferred region: {infer_region(ticker)}\n"
            f"Company name hint (SEC): {bundle.company_name_hint or 'unknown'}\n"
            f"{'=' * 72}\n"
        )
        parts.append("### Press / news transcript\n")
        parts.append(bundle.transcript_text or "(no transcript)")
        parts.append("\n### SEC-style filing excerpt\n")
        parts.append(bundle.sec_text or "(no SEC text in raw_pdfs)")
    return "\n".join(parts)


def call_openai_batch(
    client: OpenAI,
    model: str,
    chunk: dict[str, CompanyBundle],
    max_retries: int,
    conversation_log: Path | None,
    batch_index: int,
    batch_total: int,
) -> list[dict[str, Any]]:
    doc = build_batch_document(chunk)
    tickers_list = ", ".join(sorted(chunk.keys()))
    system = (
        POLICY_FILL_ALL
        + "You are a biotech equity analyst. The user message contains multiple companies, "
        "each introduced by 'COMPANY TICKER: <symbol>'. "
        "Return the structured BatchRowsResponse: rows[] must include EXACTLY one object per "
        f"required ticker, and each object's ticker must match one of: {tickers_list}. "
        "Use other companies in this batch as regional/size peers when imputing splits. "
        "All dollar fields are **thousands of USD**; impute thoughtfully when sources are sparse. "
        "Follow the schema field descriptions exactly."
    )
    user = f"""Required tickers (each exactly once; ticker field must match): {tickers_list}

=== SOURCE DATA ===
{doc}
"""
    parsed = openai_parse_with_retry(
        client,
        model,
        system,
        user,
        BatchRowsResponse,
        max_retries=max_retries,
        log_prefix=f"batch n={len(chunk)}",
        conversation_log=conversation_log,
        log_heading=(
            f"Batch request {batch_index}/{batch_total} — {len(chunk)} companies — "
            f"tickers: {tickers_list}"
        ),
    )

    by_ticker: dict[str, dict[str, Any]] = {}
    for item in parsed.rows:
        t = item.ticker.strip().upper()
        by_ticker[t] = parsed_row_to_excel(item)

    ordered: list[dict[str, Any]] = []
    for ticker in sorted(chunk.keys()):
        tu = ticker.upper()
        if tu in by_ticker:
            ordered.append(by_ticker[tu])
        else:
            ordered.append(
                error_excel_row(
                    ticker,
                    chunk[ticker],
                    "(missing from model batch response)",
                )
            )
    return ordered


def call_openai_for_company(
    client: OpenAI,
    model: str,
    ticker: str,
    bundle: CompanyBundle,
    peer_context: str,
    max_retries: int,
    conversation_log: Path | None,
) -> dict[str, Any]:
    system = (
        POLICY_FILL_ALL
        + "You are a biotech equity analyst. Extract data from the user message for one company. "
        "Use the peer section for regional comparables when imputing modality splits. "
        "Dollar fields are **thousands of USD**; estimate when filings/transcripts lack splits. "
        "Return a SingleCompanyRow matching the schema."
    )
    user = f"""Ticker: {ticker}
Company name hint (from SEC header if any): {bundle.company_name_hint or "derive from sources"}

=== Peer / regional context for estimation ===
{peer_context}

=== Press / news transcript ===
{bundle.transcript_text or "(no transcript)"}

=== SEC-style filing excerpt ===
{bundle.sec_text or "(no SEC text in raw_pdfs)"}
"""
    parsed = openai_parse_with_retry(
        client,
        model,
        system,
        user,
        SingleCompanyRow,
        max_retries=max_retries,
        log_prefix=ticker,
        conversation_log=conversation_log,
        log_heading=f"Per-company request — ticker {ticker}",
    )
    return parsed_row_to_excel(parsed)


def run(
    transcripts_dir: Path,
    pdfs_dir: Path,
    output_path: Path,
    model: str,
    dry_run: bool,
    only_tickers: set[str] | None,
    per_company: bool,
    batch_max_chars: int,
    max_companies_per_batch: int,
    inter_batch_sleep_sec: float,
    max_retries: int,
    conversation_log: Path | None,
) -> None:
    bundles = discover_companies(transcripts_dir, pdfs_dir)
    if only_tickers:
        bundles = {k: v for k, v in bundles.items() if k.upper() in only_tickers}
    if not bundles:
        raise SystemExit(
            f"No data found under {transcripts_dir} / {pdfs_dir}. "
            "Expected *_press_releases.txt and/or raw_pdfs/sec-edgar-filings/<TICKER>/..."
        )

    if dry_run:
        for t, b in sorted(bundles.items()):
            print(f"--- {t} ---")
            print(f"  transcripts: {len(b.transcript_paths)}  sec files: {len(b.sec_paths)}")
            print(f"  region: {infer_region(t)}")
            print(f"  company hint: {b.company_name_hint}")
            print(f"  transcript chars: {len(b.transcript_text)}  sec chars: {len(b.sec_text)}")
        if not per_company:
            nchunks = len(chunk_bundles(bundles, batch_max_chars, max_companies_per_batch))
            print(
                f"\n[dry-run] Batch mode would use {nchunks} OpenAI request(s) "
                f"(≤{max_companies_per_batch} companies, ≤{batch_max_chars:,} chars each)."
            )
        return

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Set OPENAI_API_KEY to your OpenAI API key.")

    base_url = os.environ.get("OPENAI_BASE_URL")
    client_kw: dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kw["base_url"] = base_url.rstrip("/")
    client = OpenAI(**client_kw)
    rows: list[dict[str, Any]] = []

    if conversation_log is not None:
        print(f"Appending LLM prompts/responses to {conversation_log.resolve()}")

    if per_company:
        for ticker, bundle in sorted(bundles.items()):
            peer_context = build_peer_context(bundles, ticker)
            try:
                row = call_openai_for_company(
                    client,
                    model,
                    ticker,
                    bundle,
                    peer_context,
                    max_retries,
                    conversation_log,
                )
            except Exception as e:
                print(f"[warn] {ticker}: {e}; writing error row.")
                row = error_excel_row(ticker, bundle, f"(error: {e})")
            rows.append(row)
            time.sleep(0.4)
    else:
        chunks = chunk_bundles(bundles, batch_max_chars, max_companies_per_batch)
        print(
            f"Batch mode: {len(bundles)} companies → {len(chunks)} OpenAI request(s) "
            f"(≤{max_companies_per_batch} companies, ≤{batch_max_chars:,} chars per request; "
            f"{inter_batch_sleep_sec:.1f}s pause between requests)."
        )
        for i, chunk in enumerate(chunks):
            try:
                rows.extend(
                    call_openai_batch(
                        client,
                        model,
                        chunk,
                        max_retries,
                        conversation_log,
                        batch_index=i + 1,
                        batch_total=len(chunks),
                    )
                )
            except Exception as e:
                print(f"[warn] batch request {i + 1}/{len(chunks)}: {e}; writing error rows.")
                for ticker, bundle in sorted(chunk.items()):
                    rows.append(error_excel_row(ticker, bundle, f"(error: {e})"))
            if i < len(chunks) - 1:
                time.sleep(inter_batch_sleep_sec)

    df = pd.DataFrame(rows, columns=EXCEL_COLUMNS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"Wrote {len(rows)} rows to {output_path.resolve()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build R&D / revenue Excel via OpenAI.")
    ap.add_argument(
        "--transcripts-dir",
        type=Path,
        default=DEFAULT_TRANSCRIPTS,
        help=f"Default: {DEFAULT_TRANSCRIPTS}",
    )
    ap.add_argument(
        "--pdfs-dir",
        type=Path,
        default=DEFAULT_PDFS,
        help=f"Default: {DEFAULT_PDFS}",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Default: {DEFAULT_OUTPUT}",
    )
    ap.add_argument(
        "--model",
        default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        help="OpenAI model id supporting structured outputs (default: gpt-4o-mini or OPENAI_MODEL)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and print stats only; do not call the API.",
    )
    ap.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated tickers to include (e.g. BIO,TMO). Default: all discovered.",
    )
    ap.add_argument(
        "--per-company",
        action="store_true",
        help="One OpenAI API call per ticker (slower; use if batch hits context limits).",
    )
    ap.add_argument(
        "--batch-max-chars",
        type=int,
        default=int(
            os.environ.get("OPENAI_BATCH_MAX_CHARS", str(DEFAULT_BATCH_MAX_CHARS))
        ),
        help=f"Soft size limit per batch request (default {DEFAULT_BATCH_MAX_CHARS}). "
        "Env: OPENAI_BATCH_MAX_CHARS.",
    )
    ap.add_argument(
        "--max-companies-per-batch",
        type=int,
        default=int(
            os.environ.get(
                "OPENAI_MAX_COMPANIES_PER_BATCH",
                str(DEFAULT_MAX_COMPANIES_PER_BATCH),
            )
        ),
        help=f"Max companies per request (default {DEFAULT_MAX_COMPANIES_PER_BATCH}). "
        "Env: OPENAI_MAX_COMPANIES_PER_BATCH.",
    )
    ap.add_argument(
        "--inter-batch-sleep",
        type=float,
        default=float(
            os.environ.get(
                "OPENAI_INTER_BATCH_SLEEP_SEC",
                str(DEFAULT_INTER_BATCH_SLEEP_SEC),
            )
        ),
        help=f"Seconds to sleep between batch requests (default {DEFAULT_INTER_BATCH_SLEEP_SEC}). "
        "Env: OPENAI_INTER_BATCH_SLEEP_SEC.",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=int(
            os.environ.get("OPENAI_MAX_RETRIES", str(DEFAULT_OPENAI_MAX_RETRIES))
        ),
        help=f"Retries per request on rate limits (default {DEFAULT_OPENAI_MAX_RETRIES}). "
        "Env: OPENAI_MAX_RETRIES.",
    )
    ap.add_argument(
        "--conversation-log",
        type=Path,
        default=DEFAULT_CONVERSATION_LOG,
        help=f"Append system/user prompts and parsed JSON responses here (default: {DEFAULT_CONVERSATION_LOG}).",
    )
    ap.add_argument(
        "--no-conversation-log",
        action="store_true",
        help="Do not write a prompt/response transcript file.",
    )
    args = ap.parse_args()
    only: set[str] | None = None
    if args.only.strip():
        only = {t.strip().upper() for t in args.only.split(",") if t.strip()}
    conv_log: Path | None = None if args.no_conversation_log else args.conversation_log
    run(
        args.transcripts_dir,
        args.pdfs_dir,
        args.output,
        args.model,
        args.dry_run,
        only,
        args.per_company,
        args.batch_max_chars,
        args.max_companies_per_batch,
        args.inter_batch_sleep,
        args.max_retries,
        conv_log,
    )


if __name__ == "__main__":
    main()
