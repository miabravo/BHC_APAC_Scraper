"""
Central configuration: APAC/ANZ inclusion, strict exclusions, modality keywords,
and LLM prompt templates for financial extraction (Gross Revenue, R&D Expenses).
"""

from __future__ import annotations

from typing import Final

# --- Regional filtering (operations / market focus) ---

# Include: Greater China, India, Japan, South Korea, Singapore, Malaysia, Thailand,
# Indonesia, Vietnam, Australia, New Zealand (and explicit sub-regions).
REGIONS_INCLUDE_KEYWORDS: Final[tuple[str, ...]] = (
    "china",
    "mainland china",
    "greater china",
    "taiwan",
    "hong kong",
    "macau",
    "india",
    "japan",
    "south korea",
    "korea",
    "singapore",
    "malaysia",
    "thailand",
    "indonesia",
    "vietnam",
    "australia",
    "new zealand",
    "apac",
    "asia-pacific",
    "asia pacific",
)

# Strictly exclude: Russia, Turkey, Iran, and all Middle Eastern countries.
REGIONS_EXCLUDE_KEYWORDS: Final[tuple[str, ...]] = (
    "russia",
    "russian",
    "turkey",
    "türkiye",
    "turkiye",
    "iran",
    "iranian",
    # Middle East (broad coverage)
    "middle east",
    "gcc",
    "saudi",
    "saudi arabia",
    "uae",
    "united arab emirates",
    "dubai",
    "abu dhabi",
    "qatar",
    "kuwait",
    "bahrain",
    "oman",
    "yemen",
    "iraq",
    "israel",
    "palestine",
    "jordan",
    "lebanon",
    "syria",
    "egypt",  # often grouped with MENA for ops; exclude per SOW
)

# Target modality / technology keywords for LLM prompts and downstream filtering.
MODALITY_KEYWORDS: Final[dict[str, str]] = {
    "aav_lv": "AAV/LV",
    "gene_therapy": "gene therapy",
    "msc_manufacturing": "MSC therapeutic manufacturing",
    "saltanase": "saltanase",
    "digital_pcr": "digital PCR",
}

# Maps every tracked ticker to a human-readable company name for Excel seeding.
TICKER_COMPANY_NAME_MAP: Final[dict[str, str]] = {
    "TMO": "Thermo Fisher Scientific",
    "BIO": "Bio-Rad Laboratories",
    "ILMN": "Illumina",
    "VRTX": "Vertex Pharmaceuticals",
    "QGEN": "Qiagen",
    "JNJ": "Johnson & Johnson",
    "PFE": "Pfizer",
    "MRK": "Merck & Co.",
    "ABT": "Abbott Laboratories",
    "DHR": "Danaher",
    "BDX": "Becton Dickinson",
    "4914.T": "Takasago International",
    "LONN.SW": "Lonza Group",
    "688238.SS": "OBiO Technology",
    "MSB.AX": "Mesoblast",
    "078160.KQ": "Medipost",
    "4978.T": "ReproCELL",
    "4527.T": "Rohto Pharmaceutical",
    "CYP.AX": "Cynata Therapeutics",
    "298060.KQ": "SCM LifeScience",
    "6869.T": "Sysmex",
    "AZT.OL": "ArcticZymes Technologies",
    "MRK.DE": "Merck KGaA",
    "688105.SS": "Vazyme Biotech",
    "301047.SZ": "Sino Biological",
    "TEM": "TEM",
    "CBLL": "CBLL",
}

# Comprehensive public tickers for press-release / news scraping (US + intl. Yahoo symbols + IPOs).
DEFAULT_PUBLIC_TICKERS: Final[tuple[str, ...]] = (
    "TMO",
    "BIO",
    "ILMN",
    "VRTX",
    "QGEN",
    "JNJ",
    "PFE",
    "MRK",
    "ABT",
    "DHR",
    "BDX",
    "4914.T",
    "LONN.SW",
    "688238.SS",
    "MSB.AX",
    "078160.KQ",
    "4978.T",
    "4527.T",
    "CYP.AX",
    "298060.KQ",
    "6869.T",
    "AZT.OL",
    "MRK.DE",
    "688105.SS",
    "301047.SZ",
    "TEM",
    "CBLL",
)

# Optional: map ticker -> investor relations press release base URL for BeautifulSoup deep scrape.
# Extend per company; leave empty to rely on yfinance news links only.
COMPANY_IR_PRESS_URLS: Final[dict[str, str]] = {
    # "TMO": "https://ir.thermofisher.com/investors/news-and-events/news/",
}

# Paths (relative to project root when run from repo root).
RAW_PDFS_DIR: Final[str] = "raw_pdfs"
VENTURE_HEALTHCARE_REPORT_FILENAME: Final[str] = "2024-annual-venture-healthcare-report.pdf"
RAW_TRANSCRIPTS_DIR: Final[str] = "raw_transcripts"
DATA_DIR: Final[str] = "data"
MASTER_COMPANIES_JSON: Final[str] = "data/target_companies_master.json"
OUTPUTS_DIR: Final[str] = "outputs"
EXCEL_OUTPUT_FILENAME: Final[str] = "qiagen_dashboard_metrics.xlsx"

# Rate limiting (seconds between external HTTP calls).
HTTP_SLEEP_SECONDS: Final[float] = 2.0
YFINANCE_SLEEP_SECONDS: Final[float] = 1.5

# Canonical pandas / Excel columns for LLM-extracted metrics (contract for JSON → DataFrame).
DASHBOARD_METRICS_DF_COLUMNS: Final[tuple[str, ...]] = (
    "Company",
    "APAC Region",
    "Modality",
    "Reported Currency",
    "Gross Revenue (Local)",
    "Gross Revenue (USD)",
    "R&D Expenses (USD)",
    "Citation",
)

_MODALITIES_LINE: Final[str] = ", ".join(
    f"'{v}'" for v in MODALITY_KEYWORDS.values()
)

LLM_SYSTEM_PROMPT: Final[str] = f"""You are a senior financial analyst AI supporting the Qiagen Market Research Dashboard. Your task is to read the user-provided text excerpts from regulatory filings (e.g. U.S. SEC Form 10-K, Form 20-F), company press releases, investor materials, and earnings call transcripts, and extract structured quantitative and geographic information with institutional-grade rigor.

## Primary extraction targets
1. **Gross Revenue** — You MUST isolate and extract **revenue that is explicitly attributable to the APAC region** (or a clearly mappable sub-region listed below). Do not substitute company-wide or global revenue where a regional breakdown is required unless the source **explicitly** states that the figure is APAC-only or a subset that falls entirely within the allowed APAC list.
2. **R&D Expenses** — Extract research and development expense figures when they are **explicitly tied** to the same geographic scope (APAC) **or** when the source states company-total R&D **and** clearly segments or discusses APAC R&D separately. If only company-wide R&D is given with **no** APAC carve-out, record that fact and do **not** assign the full amount to APAC.

## Allowed APAC / ANZ regions (only these count as valid "APAC Region" values)
Use **one** of these labels when the source supports it (or the closest explicit sub-label from the source):
- **Greater China** — mainland China, Taiwan, Hong Kong (and Macau if treated with Greater China in the source)
- **India**
- **Japan**
- **South Korea**
- **Singapore**
- **Malaysia**
- **Thailand**
- **Indonesia**
- **Vietnam**
- **Australia**
- **New Zealand**

If the document uses a different but equivalent name (e.g. "PRC", "ROC", "HK SAR"), normalize to the labels above when unambiguous.

## Strictly excluded geographies
You MUST **not** allocate revenue or R&D to APAC if the underlying disclosure is **only** for or **dominated by** excluded regions:
- **Russia** (and CIS if used as a proxy for Russia-centric revenue)
- **Turkey**
- **Iran**
- **All Middle Eastern countries** (e.g. Saudi Arabia, UAE, Qatar, Kuwait, Bahrain, Oman, Israel, Jordan, Lebanon, Iraq, Yemen, Syria, and similar)

If figures are reported only for excluded regions, **omit** them from APAC-focused rows or return nulls for APAC-specific fields with a clear explanation in **Citation**.

## EMEA and other blended regions
If revenue (or R&D) is reported only as **EMEA**, **Europe + Middle East + Africa**, or similar **without** a separate line item for APAC, Asia-Pacific, or individual allowed countries:
- You MUST **not** guess an APAC share.
- You MUST add an explicit note in **Citation** that **Middle East / Russia / Turkey / Iran could not be decoupled** from the blended region and therefore **APAC-specific revenue was not isolated**.

## Modality mapping
For each extracted row, map the disclosure to **at most one** primary modality from this closed set (choose the modality **explicitly supported** by the text; if multiple apply, prefer the most specific mention or output separate rows if the source clearly separates them):
{_MODALITIES_LINE}

If the source does not tie the financial line item to any of these modalities, set **Modality** to null and explain in **Citation**.

## Foreign currency and USD
- If **Gross Revenue** or **R&D** amounts are stated in a **non-USD** currency (e.g. **JPY**, **AUD**, **CNY**, **KRW**, **SGD**, **TWD**, **HKD**, **INR**, **EUR** where clearly regional), you MUST:
  - Set **Reported Currency** to the **ISO 4217** code (e.g. JPY, CNY).
  - Populate **Gross Revenue (Local)** with the **numeric value as reported** (no currency symbol in the cell; use a plain number).
  - Leave **Gross Revenue (USD)** and **R&D Expenses (USD)** as **null** unless the source **explicitly** states the USD equivalent or provides an **explicit** exchange rate in the same passage for you to compute it. Do **not** use market FX from memory.
  - Add a short flag in **Citation** such as `NEEDS_USD_CONVERSION: <CCY>` so downstream Python can standardize to USD.

## Anti-hallucination rules
- Extract **only** what is **directly supported** by the provided text. Cite the exact location.
- If a figure, region, or modality is **not** in the excerpt, use **null** — do **not** infer, extrapolate, or use outside knowledge.
- Preserve **fiscal period** (e.g. FY2024, Q3 2024) inside **Citation** when given.

## Output shape
Return data as a list of objects (JSON-ready) whose fields align exactly with the dashboard DataFrame columns (see module constant DASHBOARD_METRICS_DF_COLUMNS): Company, APAC Region, Modality, Reported Currency, Gross Revenue (Local), Gross Revenue (USD), R&D Expenses (USD), Citation.

**Citation** must include: source document type or file name if provided, page number or section/table identifier if available, and a one-line quote or paraphrase anchor showing why the number applies to the stated APAC Region and Modality.
"""


def build_financial_extraction_system_prompt() -> str:
    """Return the canonical system prompt for financial extraction (alias of ``LLM_SYSTEM_PROMPT``)."""
    return LLM_SYSTEM_PROMPT


def build_user_prompt_chunk(source_label: str, text_chunk: str) -> str:
    """User message wrapping one chunk of source text (e.g. transcript or filing excerpt)."""
    cols = ", ".join(DASHBOARD_METRICS_DF_COLUMNS)
    return (
        f"Source: {source_label}\n\n"
        "Extract Gross Revenue (APAC-isolated where possible) and R&D Expenses per the system "
        "instructions. Return JSON-ready rows with fields exactly matching these column names: "
        f"{cols}. Use null for unknown values.\n\n"
        f"---\n{text_chunk}\n---"
    )


def region_passes_filter(region_or_text: str) -> bool:
    """
    Return True if the region string is allowed (include match and no exclude match).
    Unknown or empty region: False (strict APAC/ANZ + exclusions policy).
    """
    if not region_or_text or not str(region_or_text).strip():
        return False
    low = str(region_or_text).lower()

    for ex in REGIONS_EXCLUDE_KEYWORDS:
        if ex in low:
            return False

    for inc in REGIONS_INCLUDE_KEYWORDS:
        if inc in low:
            return True

    return False
