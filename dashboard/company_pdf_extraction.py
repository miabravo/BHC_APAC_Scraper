"""
Dynamic company extraction from a fixed page range in the Venture Healthcare report PDF.

Uses pdfplumber for text extraction, then spaCy NER (ORG) when available, with
regex/capitalization heuristics as fallback. Merges into a master JSON list on disk.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pdfplumber

from dashboard.config import MASTER_COMPANIES_JSON, RAW_PDFS_DIR, VENTURE_HEALTHCARE_REPORT_FILENAME


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def extract_text_from_pdf_pages(
    pdf_path: Path,
    start_page_one_indexed: int,
    end_page_one_indexed_inclusive: int,
) -> dict[int, str]:
    """
    Extract text per page for inclusive page range (1-based PDF page numbers).

    Returns mapping: 1-based page number -> page text.
    """
    pages_text: dict[int, str] = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            n_pages = len(pdf.pages)
            for pnum in range(start_page_one_indexed, end_page_one_indexed_inclusive + 1):
                idx = pnum - 1
                if idx < 0 or idx >= n_pages:
                    continue
                page = pdf.pages[idx]
                t = page.extract_text() or ""
                pages_text[pnum] = t
    except Exception as e:
        raise RuntimeError(f"Failed to read PDF {pdf_path}: {e}") from e

    return pages_text


def extract_orgs_spacy(text: str) -> set[str]:
    """Named entities labeled ORG via spaCy (optional dependency)."""
    try:
        import spacy  # type: ignore
    except ImportError:
        return set()

    try:
        nlp = spacy.load("en_core_web_sm")
    except Exception:
        return set()

    doc = nlp(text[:1_000_000])  # guard extreme size
    return {ent.text.strip() for ent in doc.ents if ent.label_ == "ORG" and len(ent.text.strip()) > 1}


# Lines that look like company headings (Title Case, 2+ tokens, not sentence starters).
_HEURISTIC_ORG = re.compile(
    r"(?:^|\n)\s*([A-Z][a-zA-Z0-9&]+(?:\s+[A-Z][a-zA-Z0-9&]+){1,8})\s*(?:\n|$)"
)


def extract_orgs_heuristic(text: str) -> set[str]:
    """Fallback: capitalization / line-based candidates; filters obvious noise."""
    noise = {
        "The", "And", "For", "Our", "This", "These", "Those", "Page", "Table", "Figure",
        "January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December",
    }
    out: set[str] = set()
    for m in _HEURISTIC_ORG.finditer(text):
        cand = m.group(1).strip()
        if len(cand) < 4:
            continue
        parts = cand.split()
        if any(p in noise for p in parts):
            continue
        if len(parts) >= 2:
            out.add(cand)
    return out


def merge_company_candidates(text: str) -> list[str]:
    """Combine spaCy ORG entities and heuristic picks; dedupe preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for name in sorted(extract_orgs_spacy(text) | extract_orgs_heuristic(text)):
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(name)
    return ordered


def load_master_company_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_master_company_list(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def extract_companies_from_venture_report_pages_15_to_20(
    project_root: Path | None = None,
) -> tuple[list[str], dict[int, str]]:
    """
    Target pages 15–20 of ``2024-annual-venture-healthcare-report.pdf`` under ``raw_pdfs``.

    Returns (company_names, page_text_by_number).
    """
    root = project_root or _project_root()
    pdf_path = root / RAW_PDFS_DIR / VENTURE_HEALTHCARE_REPORT_FILENAME

    if not pdf_path.is_file():
        raise FileNotFoundError(
            f"Expected PDF at {pdf_path}. Place '{VENTURE_HEALTHCARE_REPORT_FILENAME}' in {RAW_PDFS_DIR}/."
        )

    print(f"[company_pdf_extraction] Reading pages 15–20 from: {pdf_path}")
    pages = extract_text_from_pdf_pages(pdf_path, 15, 20)
    combined = "\n\n".join(pages[p] for p in sorted(pages.keys()))

    names = merge_company_candidates(combined)
    print(f"[company_pdf_extraction] Found {len(names)} candidate company name(s).")

    # Merge into master JSON
    master_path = root / MASTER_COMPANIES_JSON
    existing = load_master_company_list(master_path)
    existing_names = {str(x.get("name", "")).lower() for x in existing if isinstance(x, dict)}
    ts = datetime.now(timezone.utc).isoformat()

    for n in names:
        if n.lower() in existing_names:
            continue
        existing.append(
            {
                "name": n,
                "source": VENTURE_HEALTHCARE_REPORT_FILENAME,
                "source_pages": "15-20",
                "added_at": ts,
            }
        )
        existing_names.add(n.lower())

    save_master_company_list(master_path, existing)
    print(f"[company_pdf_extraction] Master list updated: {master_path} ({len(existing)} total records).")

    return names, pages


def run_extraction_cli() -> None:
    """CLI entry: run extraction and print summary."""
    try:
        names, _ = extract_companies_from_venture_report_pages_15_to_20()
        for i, n in enumerate(names, 1):
            print(f"  {i}. {n}")
    except FileNotFoundError as e:
        print(f"[WARNING] {e}")
    except Exception as e:
        print(f"[ERROR] Company extraction failed: {e}")


if __name__ == "__main__":
    run_extraction_cli()
