"""
Annual Report Finder & Cleaner
1. Cleans messy US 10-K SEC filings into plain text.
2. Searches for and downloads International PDF Annual Reports.
"""

import os
import re
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None
"Enures that international tickers are mapped to the correct company name"
INTERNATIONAL_TICKER_MAP = {
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
    "301047.SZ": "Sino Biological"
}
def clean_sec_filings(sec_dir: str = "sec-edgar-filings", out_dir: str = "raw_transcripts"):
    print(f"\n[START] Looking for SEC filings in: {sec_dir}")
    sec_path = Path(sec_dir)
    out_path = Path(out_dir)
    out_path.mkdir(exist_ok=True)

    print("--- 🧹 Part 1: Cleaning US SEC 10-K Filings ---")
    files_found = False
    
    for root, dirs, files in os.walk(sec_path):
        for file in files:
            if file.endswith(".txt"):
                files_found = True
                
                # Smarter ticker extraction: look at all folder parts to find the real ticker
                parts = Path(root).parts
                ticker = "UNKNOWN"
                for p in parts:
                    # Tickers are usually short, uppercase letters
                    if p.isupper() and 1 <= len(p) <= 5 and p != "10-K":
                        ticker = p
                        break
                        
                filepath = Path(root) / file
                print(f"Cleaning 10-K for {ticker}...")
                
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    raw_text = f.read()
                
                doc_pattern = re.compile(r'<DOCUMENT>\s*<TYPE>10-K.*?<TEXT>(.*?)</TEXT>', re.DOTALL | re.IGNORECASE)
                match = doc_pattern.search(raw_text)
                html_payload = match.group(1) if match else raw_text
                
                soup = BeautifulSoup(html_payload, "html.parser")
                
                for hidden_tag in soup.find_all(['ix:header', 'table']):
                    hidden_tag.decompose()
                for tag in soup.find_all(style=re.compile(r'display:\s*none', re.I)):
                    tag.decompose()
                
                clean_text = soup.get_text(separator="\n", strip=True)
                clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)
                
                out_file = out_path / f"{ticker}_10K_clean.txt"
                out_file.write_text(clean_text[:300000], encoding='utf-8')
                print(f"  ✅ Saved clean text to {out_file.name}")
                
    if not files_found:
        print("[INFO] Searched the folder, but didn't find any .txt files to clean!")

def find_international_annual_reports(pdf_dir: str = "raw_pdfs"):
    print("\n[START] Starting International Report Search...")
    if DDGS is None:
        print("[WARNING] ddgs not installed. Run: pip install ddgs")
        return
        
    out_path = Path(pdf_dir)
    out_path.mkdir(exist_ok=True)
    
    international_suffixes = ('.T', '.SW', '.AX', '.KQ', '.SZ', '.SS', '.OL', '.DE')
    
    try:
        from dashboard.config import DEFAULT_PUBLIC_TICKERS
        tickers = [t for t in DEFAULT_PUBLIC_TICKERS if t.endswith(international_suffixes)]
    except ImportError:
        tickers = ["4527.T", "4914.T", "LONN.SW", "MSB.AX", "078160.KQ"] 
        
    print("--- 🔍 Part 2: Finding International Annual Reports ---")
    ddgs = DDGS()
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    for ticker in tickers:
        # New bulletproof logic
        search_term = INTERNATIONAL_TICKER_MAP.get(ticker, ticker)
        query = f'"{search_term}" "annual report" 2024 OR 2025 filetype:pdf'
        
        print(f"Searching for: {ticker} (Query: {query})...")
        
        try:
            results = list(ddgs.text(query, max_results=5))
            pdf_url = None
            
            for res in results:
                href = res.get('href', '').lower()
                if href.endswith('.pdf') or 'pdf' in href:
                    pdf_url = res['href']
                    break
            
            if pdf_url:
                print(f"  Downloading PDF: {pdf_url}")
                time.sleep(2)
                response = requests.get(pdf_url, headers=headers, timeout=15)
                response.raise_for_status()
                
                pdf_file = out_path / f"{ticker}_Annual_Report.pdf"
                with open(pdf_file, 'wb') as f:
                    f.write(response.content)
                print(f"  ✅ Saved to {pdf_file.name}")
            else:
                print(f"  ⚠️ No direct PDF found for {ticker}.")
        except Exception as e:
            print(f"  ❌ Failed to fetch for {ticker}: {e}")
            
        time.sleep(3)

if __name__ == "__main__":
    print("\n🚀 RUNNING ANNUAL REPORT PIPELINE 🚀")
    clean_sec_filings()
    find_international_annual_reports()
    print("🏁 PIPELINE FINISHED 🏁\n")