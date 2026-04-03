"""
Batch PDF chunker: every ``*.pdf`` under ``raw_pdfs/`` → chunked JSON in ``processed_pdfs/``.

Skips PDFs that already have a corresponding ``*_chunks.json`` output (idempotent re-runs).

For Venture Healthcare company extraction (pages 15–20), modality/regional filtering,
press scraping, and Excel export, see ``dashboard/`` and ``run_dashboard_pipeline.py``.
"""

from __future__ import annotations

import glob
import json
from pathlib import Path
from typing import List

import pdfplumber

RAW_PDFS_DIR = Path("raw_pdfs")
PROCESSED_PDFS_DIR = Path("processed_pdfs")


def find_all_pdf_paths(raw_dir: Path) -> List[Path]:
    """
    Return sorted paths to every ``.pdf`` under ``raw_dir`` (recursive), using glob.
    """
    raw_dir = raw_dir.resolve()
    pattern = str(raw_dir / "**" / "*.pdf")
    matches = sorted(glob.glob(pattern, recursive=True))
    return [Path(p) for p in matches]


def output_json_path_for_pdf(pdf_path: Path, raw_dir: Path, out_root: Path) -> Path:
    """
    Map ``raw_pdfs/sub/file.pdf`` → ``processed_pdfs/sub/file_chunks.json``.
    """
    raw_dir = raw_dir.resolve()
    pdf_path = pdf_path.resolve()
    rel = pdf_path.relative_to(raw_dir)
    target = out_root / rel.parent / f"{rel.stem}_chunks.json"
    return target


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all text from a PDF using pdfplumber."""
    all_text: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text:
                all_text.append(page_text)
    return "\n".join(all_text)


def chunk_text(text: str, chunk_size_words: int = 500, overlap_words: int = 50) -> List[dict]:
    """
    Split text into overlapping word chunks.

    - Each chunk has up to ``chunk_size_words`` words.
    - Consecutive chunks overlap by ``overlap_words`` words.
    """
    words = text.split()
    if not words:
        return []

    step = max(chunk_size_words - overlap_words, 1)
    chunks: List[dict] = []
    start = 0
    chunk_id = 0
    n_words = len(words)

    while start < n_words:
        end = min(start + chunk_size_words, n_words)
        chunk_words = words[start:end]
        chunk_text_str = " ".join(chunk_words)

        chunks.append(
            {
                "id": chunk_id,
                "start_word_index": start,
                "end_word_index": end - 1,
                "text": chunk_text_str,
            }
        )

        chunk_id += 1
        start += step

    return chunks


def save_chunks_to_json(chunks: List[dict], output_path: Path) -> None:
    """Save chunk list to UTF-8 JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)


def main() -> None:
    raw_dir = RAW_PDFS_DIR
    out_root = PROCESSED_PDFS_DIR

    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Expected directory {raw_dir} to exist (place PDFs under it)."
        )

    pdf_paths = find_all_pdf_paths(raw_dir)
    if not pdf_paths:
        print(f"No .pdf files found under {raw_dir.resolve()}. Nothing to do.")
        return

    print(f"Found {len(pdf_paths)} PDF file(s) under {raw_dir.resolve()}.\n")

    processed_count = 0
    skipped_count = 0

    for pdf_idx, pdf_path in enumerate(pdf_paths, start=1):
        out_json = output_json_path_for_pdf(pdf_path, raw_dir, out_root)

        if out_json.exists() and out_json.stat().st_size > 0:
            print(
                f"[{pdf_idx}/{len(pdf_paths)}] SKIP (already processed): {pdf_path}\n"
                f"         → {out_json}\n"
            )
            skipped_count += 1
            continue

        print(f"[{pdf_idx}/{len(pdf_paths)}] Processing: {pdf_path}")
        try:
            text = extract_text_from_pdf(pdf_path)
        except Exception as e:
            print(f"  [ERROR] Text extraction failed: {e}\n")
            continue

        word_count = len(text.split())
        print(f"  Extracted {word_count} words.")

        chunks = chunk_text(text, chunk_size_words=500, overlap_words=50)
        for i, ch in enumerate(chunks):
            ch["id"] = i
            ch["source_pdf"] = str(pdf_path)

        print(f"  Created {len(chunks)} chunk(s).")
        try:
            save_chunks_to_json(chunks, out_json)
            print(f"  Saved: {out_json.resolve()}\n")
            processed_count += 1
        except Exception as e:
            print(f"  [ERROR] Failed to write JSON: {e}\n")

    print(
        f"Done. Processed: {processed_count}, skipped (already done): {skipped_count}, "
        f"total PDFs seen: {len(pdf_paths)}."
    )


if __name__ == "__main__":
    main()
