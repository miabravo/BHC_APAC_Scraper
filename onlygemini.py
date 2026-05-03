"""
Gemini generateContent → spreadsheet (Excel).

Requires: GEMINI_API_KEY (or GOOGLE_API_KEY) in the environment.
Optional: GEMINI_MODEL (default gemini-2.5-flash).
Optional: ONLYGEMINI_USE_SEARCH_GROUNDING (default 1) — set to 0 to disable the Google Search grounding tool.
Optional: ONLYGEMINI_FORCE_JSON_WITH_GROUNDING (default 0) — set to 1 to send ``responseMimeType: application/json`` even with search grounding (many setups return 400 if both are on; leave 0 unless you know your model supports it).
Optional: ONLYGEMINI_BATCH_TIMEOUT_S (default 900) — HTTP timeout seconds for the single batch request when batch mode is on.
Optional: ONLYGEMINI_MAX_REQUESTS_PER_MIN (default 5) — rate cap for per-company mode.

Mode is controlled by ``USE_ONE_API_CALL_FOR_ENTIRE_COMPANY_LIST`` in this file:
  False — one API request per company (default).
  True — one API request for the full company list (see ``prompt_for_all_companies``).

Writes onlygemini_results.xlsx next to this file when run as __main__.
Progress is printed (disable with run_all(..., verbose=False)).
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

PROMPT_TEMPLATE = """
Use this as a single prompt in your API call:

You are conducting structured web research on **[COMPANY NAME]**, an Asia-based biotech company, to estimate revenue exposure to advanced therapy manufacturing modalities using only evidence-backed classification and minimizing hallucination.

Objective:
Scrape and analyze all available public information (annual reports, investor presentations, earnings releases, regulatory filings, company websites, manufacturing site descriptions, industry databases, news coverage, and PDFs) to estimate company revenue attributable to these buckets:

1. AAV/LV Viral Vector Manufacturing (most specific subcategory)
2. Gene Therapy Manufacturing (broader category including AAV/LV unless specifically broken out)
3. Stem Cell Therapeutic Manufacturing (separate from gene therapy)

Use evidence-driven classification only.

Keyword-Guided Evidence Detection

Direct modality terms:
AAV, adeno-associated virus, AAV vector
LV, LVV, lentiviral, lentiviral vector
viral vector, vector manufacturing, gene delivery
gene therapy, cell and gene therapy, CGT
MSC, mesenchymal stem cell, stem cell therapy
cell therapy, regenerative medicine

Manufacturing / GMP terms:
GMP, cGMP, clinical-grade, commercial-grade
manufacturing, production, process development
upstream, downstream, fill-finish
scale-up, tech transfer, validation
plasmid, transfection, packaging cell line
purification, QC, QA, release testing
bioreactor, suspension culture, adherent culture

Gene-therapy-specific process terms:
vector production
capsid, serotype
transgene
payload
helper virus
producer cell line
viral titer
potency assay
MOI, multiplicity of infection
plasmid DNA, pDNA

MSC / cell-therapy-specific terms:
mesenchymal stem cell, MSC
iMSC, hMSC
cell expansion
cell banking
cryopreservation
allogeneic, autologous
cell processing
cell harvest
master cell bank, working cell bank

Segment / business-unit terms:
CDMO, CMO, CRDMO
biologics, bioprocessing
advanced therapies
new modality, novel modality
biologics development services
contract development and manufacturing
biologics manufacturing
viral vector site
CGT site
platform, capabilities, pipeline

Research Algorithm

Step 1: Ingest
Scrape:

* Annual reports
* Investor decks
* Earnings PDFs
* Regulatory filings
* Website manufacturing pages
* Press releases / facility announcements
* Trusted secondary sources

OCR image pages.
Chunk by page or section.

Step 2: Detect Evidence
Run exact keyword matching first.
Run semantic matching second for synonyms and paraphrases.

Only classify evidence tied to manufacturing, GMP operations, modality platforms, or revenue disclosures.

Step 3: Classify into Buckets
Map each hit into one bucket:

* aav_lv
* gene_therapy
* stem_cell

Hierarchy Rules:
AAV/LV is a subset of Gene Therapy (same hierarchy must hold in all numeric outputs: AAV/LV revenue estimates must never exceed gene-therapy estimates for value or range bounds—reconcile conflicting disclosures and document in assumptions).
Stem Cell is a separate bucket, never part of Gene Therapy.

Always use the most specific available bucket:
Use AAV/LV revenue if disclosed.
Else use Gene Therapy revenue.
Else infer from CDMO/segment revenue.
Else infer from total revenue only as last resort.

Never double count parent and child categories.

Step 4: Confidence Scoring
Assign:
3 = explicit revenue or explicit GMP/site evidence
2 = strong operational evidence
1 = proxy evidence only
0 = unrelated/insufficient

Step 5: Revenue Attribution
Priority:

1. Use explicit segment revenue if available.
2. If only broad segment revenue exists, estimate modality share using benchmarks and state assumptions.
3. If only proxy evidence exists, assign low-confidence inferred revenue.

Anti-double-counting:

* Segment revenue first
* Modality split second
* Exclude supporting tools/reagents/instruments unless modeling enabling tools separately

Do not count:

* Gene therapy and AAV/LV as separate totals
* MSC inside gene therapy
* Support tools as manufacturing revenue

Core Rules

* Prefer direct manufacturing evidence over broad CDMO claims.
* Prefer report-specific evidence over generic benchmarks.
* Use benchmarks only as fallback.
* Flag assumptions explicitly.
* If segment revenue cannot be sourced exactly, still produce defensible estimates per the Spreadsheet output policy below; set insufficient_evidence true only when no credible numeric bound exists after search.
* Focus classification only when supported by keyword framework and source evidence.
* Do not infer modality exposure from vague biotech language alone.
* Every cell should still be full, don't leave it blank

Spreadsheet output policy (required for downstream analysis)
* All amounts are USD millions (plain numbers only in JSON fields; no currency symbols, no commas).
* For summary objects aav_lv_revenue_estimate, gene_therapy_revenue_estimate, stem_cell_revenue_estimate, and total_advanced_therapy_revenue: populate "value" and/or "range_low"/"range_high" whenever the company plausibly participates in that modality (including CDMO, segment, or parent revenue you can apportion). Use wide ranges when uncertain. Use null for a bucket only if exposure is effectively zero for that modality.
* Subset consistency (required): AAV/LV manufacturing is strictly inside gene therapy manufacturing. Whenever both buckets have non-null numbers, enforce aav_lv ≤ gene therapy for each comparable field: value vs value, range_low vs range_low, range_high vs range_high. If AAV/LV is non-null but gene therapy is null, do not leave gene therapy null—set gene therapy to at least the AAV/LV level (or widen its range to subsume AAV/LV). If raw disclosures appear to violate the subset ordering, reconcile by widening gene therapy or narrowing AAV/LV and explain in assumptions.
* When public figures are missing, infer from the closest disclosed segment, parent company revenue, facility scale, or peer benchmarks; state every inference in "assumptions" and set confidence accordingly (0–3).
* If your primary token/keyword-driven inference still cannot produce a numeric cell estimate, you MUST create an alternative estimate for that specific cell using a transparent fallback method (e.g., peer-ratio transfer, capacity proxy, segment apportionment, site-count heuristic, or market-share backsolve). Record the exact fallback method and arithmetic in "assumptions", and add a matching explanation in evidence.notes with reported_or_inferred="inferred".
* insufficient_evidence may be true only when search yields no usable scale signal for advanced-therapy manufacturing; in that rare case summary numbers may be null but assumptions must explain why.

Return ONLY valid JSON. No markdown. No explanatory text. No prose outside JSON.

Use exactly this schema:

{
"company": "COMPANY_NAME",
"summary": {
"aav_lv_revenue_estimate": {
"value": null,
"range_low": null,
"range_high": null,
"confidence": 0
},
"gene_therapy_revenue_estimate": {
"value": null,
"range_low": null,
"range_high": null,
"confidence": 0
},
"stem_cell_revenue_estimate": {
"value": null,
"range_low": null,
"range_high": null,
"confidence": 0
},
"total_advanced_therapy_revenue": {
"value": null,
"range_low": null,
"range_high": null
}
},
"evidence": [
{
"url": "",
"source": "",
"page": "",
"keywords": [],
"bucket": "",
"confidence": 0,
"reported_or_inferred": "",
"estimated_revenue": {
"value": null,
"range_low": null,
"range_high": null
},
"notes": ""
}
],
"assumptions": [],
"insufficient_evidence": false
}

Field Rules:

* Revenue values must be numeric only (no currency symbols or commas)
* Every object in "evidence" must include "url" set to the full canonical https (or http) URL of the specific page, filing, or PDF viewer location where the cited evidence appears; do not omit url or use placeholders for real citations. Use "" only when the evidence array is empty. "source" may hold a short human-readable document or site title alongside url.
* For other evidence string fields use "" or [] when unknown; for summary revenue numerics follow the Spreadsheet output policy (avoid all-null summaries when any plausible exposure exists)
* confidence must be 0,1,2,or 3 only

bucket must be exactly one of:
"aav_lv"
"gene_therapy"
"stem_cell"

reported_or_inferred must be exactly one of:
"reported"
"inferred"

If no evidence exists return:

{
"company": "COMPANY_NAME",
"summary": {},
"evidence": [],
"assumptions": [],
"insufficient_evidence": true
}

Do not fabricate missing segment splits.
Do not output anything except JSON.
"""

COMPANIES = [
    "WuXi Advanced Therapies",
    "Takara Bio",
    "Lonza Group (APAC Viral Div.)",
    "GenScript ProBio",
    "Obio Technology"
]
"""
    "Porton Advanced Solutions",
    "Thermo Fisher (Viral Vector Svcs)",
    "Mesoblast Limited",
    "Medipost Co., Ltd.",
    "REPROCELL",
    "Rohto Pharmaceutical",
    "Cynata Therapeutics",
    "SCM Lifescience",
    "Steminent Biotherapeutics",
    "Magellan Stem Cells",
    "Bio-Rad Laboratories",
    "Sysmex Corporation",
    "JN Medsys",
    "Combinati",
    "Stilla Technologies",
    "TargetingOne",
    "Optolane",
    "Pilot Gene",
    "ArcticZymes Technologies",
    "Merck KGaA (MilliporeSigma)",
    "New England Biolabs (NEB)",
    "c-LEcta",
    "Vazyme Biotech",
    "Sino Biological",
    "Yeasen Biotechnology",
    "Xaira Therapeutics",
    "Tempus",
    "Ceribell",
    "GondolaBio"
]
"""

# If False: one generateContent request per company. If True: one request for all companies
# in ``COMPANIES`` (or the list passed to ``run_all``), with a batch prompt and root JSON
# ``{"companies": [ ... ]}``.
# True = one huge request (slower, one shared grounding blob). False = one request per company
# (recommended: clearer JSON, per-row grounding, fewer parse failures).
USE_ONE_API_CALL_FOR_ENTIRE_COMPANY_LIST = False

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:generateContent"
)
OUTPUT_XLSX = Path(__file__).resolve().parent / "onlygemini_results.xlsx"


def prompt_for_company(company: str) -> str:
    """Replace display name and JSON placeholder company string safely."""
    json_company = json.dumps(company)[1:-1]
    return (
        PROMPT_TEMPLATE.replace("[COMPANY NAME]", company).replace(
            "COMPANY_NAME", json_company
        )
    )


def prompt_for_all_companies(companies: list[str]) -> str:
    """
    One prompt covering every company. Reuses the shared methodology block from
    ``PROMPT_TEMPLATE`` (from ``Objective:`` through the line before the single-company
    JSON example) and the same schema / field rules tail, but requires a root object
    ``{"companies": [ ... ]}`` with one element per company in list order.
    """
    if not companies:
        raise ValueError("companies must be non-empty")
    t = PROMPT_TEMPLATE
    obj_start = t.index("Objective:")
    schema_key = "\nUse exactly this schema:"
    schema_start = t.index(schema_key)
    objective_block = t[obj_start:schema_start]
    schema_tail = t[schema_start + len(schema_key) :].lstrip("\n")
    numbered = "\n".join(f"{i + 1}. {name}" for i, name in enumerate(companies))
    n = len(companies)
    return f"""Use this as a single prompt in your API call:

You are conducting structured web research on ALL of the following Asia-based biotech companies in one task. For each company, apply the methodology below independently. Do not pool, merge, or reallocate revenue or evidence across companies.

Companies — fixed order ({n} total). Your JSON array ``companies`` MUST have exactly this many elements, in this exact order (position i holds the result for list entry i):

{numbered}

{objective_block}

BATCH OUTPUT (required root shape)
Return a single JSON object with exactly one top-level key: "companies".
Its value must be a JSON array of length exactly {n}.
For each index i (0-based), companies[i] must be the complete per-company object for the company at line i+1 in the numbered list above, and that object must include "company" set to that exact string.
Each companies[i] MUST be a JSON object embedded in the array — never a quoted JSON string, never markdown, never a sub-array wrapping the object.

Each per-company object must obey the same structure, field rules, and insufficient-evidence rules as in the single-company schema below.

Use exactly this schema:
{schema_tail}

FINAL REMINDER: Respond with one JSON object only, of the form {{"companies": [<{n} per-company objects>]}}. No markdown. No prose outside JSON. No top-level keys other than "companies".
"""


def flatten_json(obj: Any, parent_key: str = "") -> dict[str, Any]:
    """One spreadsheet column per leaf key; lists serialized as JSON strings."""
    items: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_json(v, nk))
            elif isinstance(v, list):
                items[nk] = json.dumps(v)
            else:
                items[nk] = v
    return items


def extract_json_from_text(text: str) -> Any:
    """Parse model text as JSON; tolerate markdown fences and leading/trailing prose."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```\s*$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    dec = json.JSONDecoder()
    # Prefer object that declares "companies" (batch) or "company" (single)
    for needle in ('"companies"', "'companies'", '"company"'):
        pos = text.find(needle)
        if pos == -1:
            continue
        brace = text.rfind("{", 0, pos)
        if brace == -1:
            continue
        try:
            obj, _end = dec.raw_decode(text[brace:])
            return obj
        except json.JSONDecodeError:
            continue
    for m in re.finditer(r"[\{\[]", text):
        try:
            obj, _end = dec.raw_decode(text[m.start() :])
            return obj
        except json.JSONDecodeError:
            continue
    raise json.JSONDecodeError("No JSON object or array found in model text", text, 0)


def _normalize_company_entry(raw: Any) -> dict[str, Any] | None:
    """Turn batch slot into a dict (handles double-encoded JSON and one-element lists)."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            inner = json.loads(s)
        except json.JSONDecodeError:
            return None
        return _normalize_company_entry(inner)
    if isinstance(raw, list):
        if len(raw) == 1:
            return _normalize_company_entry(raw[0])
        dicts = [x for x in raw if isinstance(x, dict)]
        if len(dicts) == 1:
            return dicts[0]
    return None


def _extract_companies_array(parsed: Any) -> list[Any] | None:
    """Return the batch array payload from several shapes the model may return."""
    if isinstance(parsed, list):
        return parsed
    if not isinstance(parsed, dict):
        return None
    for key in ("companies", "results", "data", "items", "output"):
        v = parsed.get(key)
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                inner = json.loads(v.strip())
            except json.JSONDecodeError:
                continue
            if isinstance(inner, list):
                return inner
            if isinstance(inner, dict) and "companies" in inner:
                return _extract_companies_array(inner)
    if parsed and all(isinstance(v, dict) for v in parsed.values()):
        if all(c in parsed for c in companies):
            return [parsed[c] for c in companies]
    return None


def rows_from_batch_response(parsed: Any, companies: list[str]) -> list[dict[str, Any]]:
    """Build one flat row per requested company from batch JSON."""
    arr = _extract_companies_array(parsed)
    if arr is None:
        raise ValueError(
            'Expected JSON with a list field like "companies" or a top-level JSON array'
        )
    if len(arr) == 1 and isinstance(arr[0], str):
        try:
            inner = json.loads(arr[0])
        except json.JSONDecodeError:
            inner = None
        if isinstance(inner, list):
            arr = inner
        elif isinstance(inner, dict):
            arr2 = _extract_companies_array(inner)
            if arr2 is not None:
                arr = arr2

    used_indices: set[int] = set()
    out: list[dict[str, Any]] = []

    def pick_for_index(i: int, company: str) -> dict[str, Any] | None:
        if i < len(arr):
            obj = _normalize_company_entry(arr[i])
            if obj is not None:
                c = str(obj.get("company", "")).strip()
                if c == company or c == "":
                    used_indices.add(i)
                    return obj
        for j, raw in enumerate(arr):
            if j in used_indices:
                continue
            obj = _normalize_company_entry(raw)
            if not obj:
                continue
            if str(obj.get("company", "")).strip() == company:
                used_indices.add(j)
                return obj
        return None

    for i, company in enumerate(companies):
        obj = pick_for_index(i, company)
        if obj is None:
            got: str
            if i < len(arr):
                got = f"{type(arr[i]).__name__}: {repr(arr[i])[:200]}"
            else:
                got = "index past end of array"
            out.append(
                {
                    "company": company,
                    "_error": f"Batch slot {i} not usable as object ({got}; len={len(arr)})",
                }
            )
        else:
            out.append(flatten_json(obj))
    return out


def compact_grounding_for_export(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Drop bulky HTML/CSS from searchEntryPoint.renderedContent for spreadsheet cells.
    Grounding chunks and supports remain for audit.
    """
    if not isinstance(meta, dict):
        return meta
    out = dict(meta)
    se = out.get("searchEntryPoint")
    if isinstance(se, dict):
        slim = {k: v for k, v in se.items() if k != "renderedContent"}
        slim["renderedContent_omitted"] = True
        out["searchEntryPoint"] = slim
    return out


def grounding_metadata_json(meta: dict[str, Any] | None) -> str | None:
    if meta is None:
        return None
    compact = compact_grounding_for_export(meta)
    return json.dumps(compact, ensure_ascii=False)


def _to_float_maybe(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _normalize_numeric(v: Any) -> Any:
    n = _to_float_maybe(v)
    if n is None:
        return v
    if n.is_integer():
        return int(n)
    return round(n, 2)


def ensure_final_estimates(row: dict[str, Any]) -> dict[str, Any]:
    """
    Fill missing summary *_value from range bounds when the model only returns
    low/high. This keeps a single concrete estimate column populated.
    """
    prefixes = [
        "summary_aav_lv_revenue_estimate",
        "summary_gene_therapy_revenue_estimate",
        "summary_stem_cell_revenue_estimate",
        "summary_total_advanced_therapy_revenue",
    ]
    for prefix in prefixes:
        v_key = f"{prefix}_value"
        lo_key = f"{prefix}_range_low"
        hi_key = f"{prefix}_range_high"
        v = _to_float_maybe(row.get(v_key))
        lo = _to_float_maybe(row.get(lo_key))
        hi = _to_float_maybe(row.get(hi_key))
        if v is None:
            if lo is not None and hi is not None:
                row[v_key] = _normalize_numeric((lo + hi) / 2.0)
            elif lo is not None:
                row[v_key] = _normalize_numeric(lo)
            elif hi is not None:
                row[v_key] = _normalize_numeric(hi)
        else:
            row[v_key] = _normalize_numeric(v)
    return row


def _text_from_candidate(candidate: dict[str, Any]) -> str:
    parts = (candidate.get("content") or {}).get("parts") or []
    chunks = [p.get("text", "") for p in parts if isinstance(p, dict) and "text" in p]
    text = "".join(chunks).strip()
    if not text:
        raise RuntimeError(f"No text in response candidate: {candidate!r}")
    return text


def call_gemini(
    prompt: str,
    api_key: str,
    model: str,
    *,
    use_search_grounding: bool = True,
    timeout_s: int = 180,
) -> tuple[str, dict[str, Any] | None]:
    """
    Call generateContent. When use_search_grounding is True, enables the
    ``google_search`` tool (Grounding with Google Search). Returns model text
    and optional groundingMetadata from the first candidate.

    When grounding is on, ``responseMimeType: application/json`` is omitted by
    default (API often rejects the combination); the prompt still asks for JSON
    and ``extract_json_from_text`` parses it.
    """
    url = GEMINI_URL.format(model=model)
    params = {"key": api_key}
    generation_config: dict[str, Any] = {"temperature": 0.2}
    # Built-in tools (e.g. google_search) with responseMimeType application/json
    # often produce HTTP 400 on the Generative Language API; omit JSON MIME when
    # grounding is on unless explicitly forced.
    force_json_with_tools = (
        os.environ.get("ONLYGEMINI_FORCE_JSON_WITH_GROUNDING", "0").strip().lower()
        in ("1", "true", "yes", "on")
    )
    if (not use_search_grounding) or force_json_with_tools:
        generation_config["responseMimeType"] = "application/json"

    body: dict[str, Any] = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": generation_config,
    }
    if use_search_grounding:
        body["tools"] = [{"google_search": {}}]

    r = requests.post(
        url,
        params=params,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=timeout_s,
    )
    if not r.ok:
        try:
            err_detail: Any = r.json()
        except Exception:
            err_detail = (r.text or "")[:8000]
        raise RuntimeError(
            f"Gemini API HTTP {r.status_code} for {url.split('?')[0]}: {err_detail!r}"
        ) from None
    data = r.json()
    candidates = data.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"No candidates in response: {data!r}")
    cand0 = candidates[0]
    grounding = cand0.get("groundingMetadata")
    if isinstance(grounding, dict):
        meta: dict[str, Any] | None = grounding
    else:
        meta = None
    return _text_from_candidate(cand0), meta


def run_all(
    companies: list[str] | None = None,
    output_path: Path | None = None,
    delay_s: float = 0.0,
    verbose: bool = True,
    use_search_grounding: bool | None = None,
) -> pd.DataFrame:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set GEMINI_API_KEY or GOOGLE_API_KEY in the environment."
        )
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    if use_search_grounding is None:
        use_search_grounding = (
            os.environ.get("ONLYGEMINI_USE_SEARCH_GROUNDING", "1").strip().lower()
            not in ("0", "false", "no", "off")
        )
    companies = companies if companies is not None else COMPANIES
    out = output_path or OUTPUT_XLSX
    total = len(companies)
    try:
        max_rpm = float(os.environ.get("ONLYGEMINI_MAX_REQUESTS_PER_MIN", "5"))
    except ValueError:
        max_rpm = 5.0
    min_interval_s = (60.0 / max_rpm) if max_rpm > 0 else 0.0
    effective_delay_s = max(delay_s, min_interval_s)

    def log(msg: str) -> None:
        if verbose:
            print(msg, flush=True)

    log(
        f"Model: {model} | Google Search grounding: "
        f"{'on' if use_search_grounding else 'off'} | "
        f"Batch single request: {'on' if USE_ONE_API_CALL_FOR_ENTIRE_COMPANY_LIST else 'off'}"
    )
    if not USE_ONE_API_CALL_FOR_ENTIRE_COMPANY_LIST:
        log(
            "Per-company pacing: "
            f"{effective_delay_s:.2f}s between request starts "
            f"(max {max_rpm:g} req/min, delay_s={delay_s})"
        )

    rows: list[dict[str, Any]] = []

    if USE_ONE_API_CALL_FOR_ENTIRE_COMPANY_LIST:
        log(f"Starting one API call for all {total} companies…")
        try:
            prompt = prompt_for_all_companies(companies)
            batch_timeout = int(os.environ.get("ONLYGEMINI_BATCH_TIMEOUT_S", "900"))
            raw, grounding_meta = call_gemini(
                prompt,
                api_key=api_key,
                model=model,
                use_search_grounding=use_search_grounding,
                timeout_s=batch_timeout,
            )
            parsed = extract_json_from_text(raw)
            flat_list = rows_from_batch_response(parsed, companies)
            for company, row in zip(companies, flat_list):
                row = dict(row)
                row = ensure_final_estimates(row)
                gm = grounding_metadata_json(grounding_meta)
                if gm is not None:
                    row["_grounding_metadata"] = gm
                row["_requested_company"] = company
                rows.append(row)
            log("Finished one API call for all companies.")
        except Exception as e:
            log(f"Batch API call failed: {e}")
            for company in companies:
                rows.append(
                    {
                        "_requested_company": company,
                        "company": company,
                        "_error": str(e),
                    }
                )
    else:
        last_request_started: float | None = None
        for i, company in enumerate(companies):
            n = i + 1
            if last_request_started is not None and effective_delay_s > 0:
                elapsed = time.monotonic() - last_request_started
                wait_s = effective_delay_s - elapsed
                if wait_s > 0:
                    log(
                        f"[{n}/{total}] Waiting {wait_s:.2f}s to respect rate limit…"
                    )
                    time.sleep(wait_s)
            log(f"[{n}/{total}] Starting: {company}")
            prompt = prompt_for_company(company)
            try:
                last_request_started = time.monotonic()
                raw, grounding_meta = call_gemini(
                    prompt,
                    api_key=api_key,
                    model=model,
                    use_search_grounding=use_search_grounding,
                )
                parsed = extract_json_from_text(raw)
                row = flatten_json(parsed)
                row = ensure_final_estimates(row)
                gm = grounding_metadata_json(grounding_meta)
                if gm is not None:
                    row["_grounding_metadata"] = gm
                log(f"[{n}/{total}] Finished: {company}")
            except Exception as e:
                row = {
                    "company": company,
                    "_error": str(e),
                }
                log(f"[{n}/{total}] Finished (error): {company} — {e}")
            row["_requested_company"] = company
            rows.append(row)

    log(f"Writing spreadsheet: {out}")
    df = pd.DataFrame(rows)
    df = df.reindex(sorted(df.columns), axis=1)
    df.to_excel(out, index=False)
    log(f"Done. Wrote {len(df)} row(s).")
    return df


if __name__ == "__main__":
    run_all()
