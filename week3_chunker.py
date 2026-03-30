import json
from pathlib import Path
from typing import List

import pdfplumber


def find_dummy_pdf(raw_dir: Path) -> Path:
    """
    Find a (dummy) PDF in the raw_pdfs directory.
    Uses the first *.pdf it finds.
    """
    pdf_files = sorted(raw_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"No PDF files found in {raw_dir}")
    return pdf_files[0]


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


def chunk_text(
    text: str, chunk_size_words: int = 1000, overlap_words: int = 100
) -> List[dict]:
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

    pdf_path = find_dummy_pdf(raw_dir)
    print(f"Reading PDF: {pdf_path}")

    text = extract_text_from_pdf(pdf_path)
    print(f"Extracted {len(text.split())} words from PDF.")

    chunks = chunk_text(text, chunk_size_words=1000, overlap_words=100)
    print(f"Created {len(chunks)} chunks.")

    output_path = Path("processed_text.json")
    save_chunks_to_json(chunks, output_path)
    print(f"Saved chunks to {output_path.resolve()}")


if __name__ == "__main__":
    main()

