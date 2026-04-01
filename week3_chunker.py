"""
Batch PDF chunker for all ``*.pdf`` under ``raw_pdfs/``.

For Venture Healthcare report company extraction (pages 15–20), modality/regional
filtering, press scraping, currency conversion, and Excel export with citations,
see the ``dashboard/`` package and ``run_dashboard_pipeline.py``.
"""

import json
from pathlib import Path
from typing import List

import pdfplumber


def find_all_pdfs(raw_dir: Path) -> List[Path]:
    """
    Find all PDF files in the raw_pdfs directory.
    """
    pdf_files = sorted(raw_dir.rglob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"No PDF files found in {raw_dir}")
    return pdf_files


def extract_text_from_pdf(pdf_path: Path) -> str:
    """
    Extract all text from a PDF using pdfplumber.
    """
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

    - Each chunk has up to `chunk_size_words` words.
    - Consecutive chunks overlap by `overlap_words` words so that
      sentences that cross boundaries are still captured.
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
    """
    Save the list of chunks to a JSON file.
    """
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)


def main() -> None:
    raw_dir = Path("raw_pdfs")
    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Expected directory {raw_dir} to exist with at least one dummy PDF."
        )

    pdf_paths = find_all_pdfs(raw_dir)
    print(f"Found {len(pdf_paths)} PDF file(s) in {raw_dir.resolve()}.\n")

    master_chunks: List[dict] = []
    global_chunk_id = 0

    for pdf_idx, pdf_path in enumerate(pdf_paths, start=1):
        print(f"[{pdf_idx}/{len(pdf_paths)}] Processing PDF: {pdf_path}")
        text = extract_text_from_pdf(pdf_path)
        word_count = len(text.split())
        print(f"  Extracted {word_count} words.")

        chunks = chunk_text(text, chunk_size_words=500, overlap_words=50)
        print(f"  Created {len(chunks)} chunk(s).\n")

        for chunk in chunks:
            chunk["id"] = global_chunk_id
            chunk["source_pdf"] = str(pdf_path)
            master_chunks.append(chunk)
            global_chunk_id += 1

    output_path = Path("master_processed_text.json")
    save_chunks_to_json(master_chunks, output_path)
    print(f"Saved chunks to {output_path.resolve()}")


if __name__ == "__main__":
    main()

