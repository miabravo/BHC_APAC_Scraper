"""
Private-company news scraper via Google News RSS + article body via newspaper3k.

For each target company, builds a quoted exact-match search query, fetches the public RSS
feed (URL-encoded query), parses the first 3 <item> entries, then downloads each story with
``newspaper.Article`` (handles Google News redirect URLs better than raw HTML parsing).

Outputs: ``raw_transcripts/{sanitized_company_name}_news.txt``
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from newspaper import Article

from dashboard.config import RAW_TRANSCRIPTS_DIR

# Master list of private target companies (Qiagen Market Research Dashboard).
PRIVATE_TARGET_COMPANIES: tuple[str, ...] = (
    "WuXi Advanced Therapies",
    "GenScript ProBio",
    "Porton Advanced Solutions",
    "Steminent Biotherapeutics",
    "Magellan Stem Cells",
    "JN Medsys",
    "Combinati",
    "Stilla Technologies",
    "TargetingOne",
    "Optolane",
    "Pilot Gene",
    "New England Biolabs",
    "c-LEcta",
    "Yeasen Biotechnology",
    "Xaira Therapeutics",
    "GondolaBio",
)

RATE_LIMIT_BETWEEN_COMPANIES_SEC: float = 3.0
REQUEST_TIMEOUT_SEC: float = 25.0
TOP_ARTICLES: int = 3

# When newspaper3k cannot extract body text, still persist RSS metadata for downstream LLM use.
PAYWALL_JS_FALLBACK_MESSAGE: str = (
    "[Body text blocked by paywall/JS. AI: Please infer market context from the Title and Date.]"
)

# Standard desktop browser User-Agent (Windows) to reduce RSS / article blocks.
BROWSER_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _sanitize_company_filename(company_name: str) -> str:
    """Safe filename stem; keeps readability, strips path-risk characters."""
    s = company_name.strip()
    for ch in '\\/:*?"<>|':
        s = s.replace(ch, "_")
    s = re.sub(r"\s+", " ", s).strip()
    return s or "unknown_company"


def build_search_query(company_name: str) -> str:
    """Double-quote the company name so Google matches the phrase, not partial tokens."""
    return f'"{company_name}" AND (revenue OR APAC OR funding OR expansion)'


def fetch_google_news_rss(query: str, limit: int = TOP_ARTICLES) -> list[dict[str, str]]:
    """
    GET Google News RSS for ``query`` with a URL-encoded ``q`` parameter.

    Parses XML with BeautifulSoup (``'xml'`` parser), returns up to ``limit`` items
    with ``title``, ``link``, and ``pubDate`` from each ``<item>``.
    """
    encoded_q = quote(query, safe="")
    url = f"https://news.google.com/rss/search?q={encoded_q}&hl=en-US&gl=US&ceid=US:en"

    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SEC,
            allow_redirects=True,
        )
        response.raise_for_status()
    except Exception as e:
        print(f"  [WARNING] Google News RSS GET failed: {e}")
        return []

    try:
        soup = BeautifulSoup(response.content, "xml")
    except Exception as e:
        print(f"  [WARNING] BeautifulSoup XML parse failed: {e}")
        return []

    items = soup.find_all("item", limit=limit)
    out: list[dict[str, str]] = []

    for it in items:
        try:
            title_el = it.find("title")
            link_el = it.find("link")
            pub_el = it.find("pubDate")
            title = title_el.get_text(strip=True) if title_el else ""
            # Some feeds put link in <link> text; Google uses <link>url</link>
            link = ""
            if link_el:
                link = link_el.get_text(strip=True) or (link_el.string or "").strip()
            pub_date = pub_el.get_text(strip=True) if pub_el else ""
            out.append(
                {
                    "title": title,
                    "link": link,
                    "pubDate": pub_date,
                }
            )
        except Exception as e:
            print(f"  [WARNING] Skipping malformed RSS <item>: {e}")
            continue

    return out


def fetch_article_text_newspaper(url: str) -> str | None:
    """
    Download and parse an article URL with newspaper3k (follows redirects, e.g. Google News).

    Returns extracted text, or ``None`` if download/parse fails or body is empty (caller
    should persist RSS metadata plus PAYWALL_JS_FALLBACK_MESSAGE).
    """
    if not url or not str(url).strip():
        return None

    try:
        article = Article(url)
        article.download()
        article.parse()
        text = (article.text or "").strip()
        if not text:
            return None
        if len(text) > 400_000:
            text = text[:400_000] + "\n\n[TRUNCATED]"
        return text
    except Exception as e:
        print(f"    [WARNING] newspaper Article failed ({url[:70]}...): {e}")
        return None


def append_metadata_fallback_block(
    lines: list[str],
    title: str,
    link: str,
    pub_date: str,
) -> None:
    """Append RSS fields + paywall/JS hint when full article text is unavailable."""
    lines.append("--- Metadata preserved (body unavailable) ---")
    lines.append(f"<title>{title}</title>")
    lines.append(f"<link>{link}</link>")
    lines.append(f"<pubDate>{pub_date}</pubDate>")
    lines.append(PAYWALL_JS_FALLBACK_MESSAGE)


def build_company_news_document(company_name: str) -> str:
    """Assemble consolidated text file content for one company."""
    query = build_search_query(company_name)
    lines: list[str] = [
        f"company: {company_name}",
        f"google_news_rss_query: {query}",
        f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    try:
        articles = fetch_google_news_rss(query, limit=TOP_ARTICLES)
    except Exception as e:
        lines.append(f"[ERROR] Could not fetch Google News RSS: {e}")
        return "\n".join(lines)

    if not articles:
        lines.append("[INFO] No <item> entries returned from Google News RSS for this query.")
        return "\n".join(lines)

    for idx, art in enumerate(articles, start=1):
        title = art.get("title", "")
        url = art.get("link", "")
        pub_date = art.get("pubDate", "N/A")

        lines.append("=" * 72)
        lines.append(f"Article {idx} of {len(articles)}")
        lines.append(f"Title: {title}")
        lines.append(f"URL: {url}")
        lines.append(f"Published (RSS pubDate): {pub_date}")
        lines.append("")

        if url:
            full_text = fetch_article_text_newspaper(url)
            if full_text:
                lines.append("--- Full article text (newspaper3k) ---")
                lines.append(full_text)
            else:
                append_metadata_fallback_block(lines, title, url, pub_date)
        else:
            lines.append("[No link for this RSS item.]")

        lines.append("")

        # Pace article fetches (including after failures) before the next URL.
        if idx < len(articles):
            time.sleep(2)

    return "\n".join(lines)


def save_company_news_file(
    company_name: str,
    content: str,
    project_root: Path | None = None,
) -> Path:
    stem = _sanitize_company_filename(company_name)
    root = project_root or _project_root()
    out_dir = root / RAW_TRANSCRIPTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{stem}_news.txt"
    path.write_text(content, encoding="utf-8")
    return path


def run_private_news_scraper(
    companies: tuple[str, ...] | list[str] | None = None,
    project_root: Path | None = None,
) -> list[Path]:
    """
    Loop all private companies, write ``{company}_news.txt`` per company.

    ``time.sleep(3)`` between companies (after each company finishes).
    """
    seq = list(companies) if companies is not None else list(PRIVATE_TARGET_COMPANIES)
    root = project_root or _project_root()
    written: list[Path] = []

    print(f"[private_news] Starting scrape for {len(seq)} private companies.\n")

    for i, company in enumerate(seq):
        print(f"[private_news] ({i + 1}/{len(seq)}) Company: {company}")
        try:
            doc = build_company_news_document(company)
            path = save_company_news_file(company, doc, project_root=root)
            written.append(path)
            print(f"  Saved: {path.name}")
        except Exception as e:
            print(f"  [WARNING] Failed for {company}: {e}")
            try:
                stub = (
                    f"company: {company}\n"
                    f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}\n\n"
                    f"[ERROR] {e}\n"
                )
                path = save_company_news_file(company, stub, project_root=root)
                written.append(path)
            except Exception as e2:
                print(f"  [WARNING] Could not write stub file: {e2}")

        if i < len(seq) - 1:
            print(f"  Sleeping {RATE_LIMIT_BETWEEN_COMPANIES_SEC}s before next company...\n")
            time.sleep(RATE_LIMIT_BETWEEN_COMPANIES_SEC)

    print(f"[private_news] Done. Wrote {len(written)} file(s) under {RAW_TRANSCRIPTS_DIR}/.")
    return written


def main() -> None:
    run_private_news_scraper()


if __name__ == "__main__":
    main()
