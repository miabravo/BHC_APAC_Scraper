#!/bin/bash

echo "🛑 PHASE 1: Wiping old data for a clean slate..."
# We delete the old folders so we know 100% of the new data is fresh
rm -rf raw_pdfs raw_transcripts sec-edgar-filings
mkdir raw_pdfs raw_transcripts sec-edgar-filings
echo "✅ Old data wiped. Folders reset."
echo ""

echo "🚀 PHASE 2: STARTING FULL DATA PIPELINE 🚀"

# This function runs a script and automatically catches if it crashes
run_and_verify() {
    echo "---------------------------------------------------"
    echo "▶️ STARTING: $1"
    
    # Run the command passed to the function
    $2
    
    # Check the "exit code" of the last command. 0 means success.
    if [ $? -ne 0 ]; then
        echo "❌ CRITICAL ERROR: $1 failed or crashed!"
        echo "🛑 Pipeline stopped. Check the logs above to fix the issue."
        exit 1
    else
        echo "✅ SUCCESS: $1 finished without crashing."
    fi
}

# Run the 4 core data engineering scripts in order
run_and_verify "SEC 10-K Downloader" "python -m week2_downloader"
run_and_verify "Public Press Releases" "python -m dashboard.press_earnings_scraper"
run_and_verify "Private Company News" "python -m dashboard.private_news_scraper"
run_and_verify "Annual Report Cleaner & Intl PDFs" "python -m dashboard.annual_report_pipeline"

echo "---------------------------------------------------"
echo "🎉 ALL PIPELINES COMPLETED SUCCESSFULLY! 🎉"
echo ""
echo "📊 DATA AUDIT (File Counts):"
echo "Text Documents (raw_transcripts): $(ls -1 raw_transcripts 2>/dev/null | wc -l | tr -d ' ')"
echo "PDF Documents (raw_pdfs): $(ls -1 raw_pdfs 2>/dev/null | wc -l | tr -d ' ')"
echo "SEC Company Folders (sec-edgar-filings): $(ls -1 sec-edgar-filings 2>/dev/null | wc -l | tr -d ' ')"
echo "---------------------------------------------------"