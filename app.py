import streamlit as st
import pandas as pd
import subprocess
import sys
from io import BytesIO
from pathlib import Path

from dashboard.config import EXCEL_OUTPUT_FILENAME, OUTPUTS_DIR

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Qiagen Intel Pipeline", page_icon="🧬", layout="wide")
PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_XLSX_PATH = PROJECT_ROOT / OUTPUTS_DIR / EXCEL_OUTPUT_FILENAME

# --- INITIALIZE SESSION STATE ---
# We do this at the very top so the app knows the key exists before it draws anything
if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = 0

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ App Controls")
    st.markdown("Use this panel to manage your session.")
    
    # The Reset Button
    if st.button("🔄 Reset App & Clear Data", use_container_width=True, type="secondary"):
        st.session_state.clear()
        # Change the uploader key to force it to render a fresh upload box
        st.session_state["uploader_key"] = 1 
        st.rerun()

# --- HEADER ---
st.title("🧬 Competitor Intelligence Analyst")
st.markdown("Welcome to the Qiagen Competitive Intel Platform. Upload any new competitor PDFs or text files, run the extraction pipeline, and download your structured insights.")
st.divider()

# --- MAIN LAYOUT ---
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("1. Upload Documents (Optional)")
    st.markdown("Drop ad-hoc PDFs or press releases here to add them to the pipeline.")
    
    # This is the SINGLE correct uploader, properly tied to the reset key
    uploaded_files = st.file_uploader(
        "Upload PDFs or TXT files", 
        type=["pdf", "txt"], 
        accept_multiple_files=True,
        key=st.session_state["uploader_key"] 
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files staged for extraction.")

with col2:
    st.subheader("2. Run AI Extraction")
    st.markdown("Trigger the master pipeline to scrape new data and run the AI extractor.")
    if st.button("🚀 Run Master Pipeline", use_container_width=True):
        with st.status("Running Pipeline...", expanded=True) as status:
            st.write("🚀 Running run_dashboard_pipeline.main() in subprocess...")
            result = subprocess.run(
                [sys.executable, "-c", "from run_dashboard_pipeline import main; main()"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                status.update(label="Pipeline failed", state="error", expanded=True)
                st.error("Pipeline execution failed.")
                if result.stderr.strip():
                    st.code(result.stderr, language="text")
                elif result.stdout.strip():
                    st.code(result.stdout, language="text")
                st.session_state["pipeline_run"] = False
            else:
                status.update(label="Extraction Complete!", state="complete", expanded=False)
                if result.stdout.strip():
                    st.code(result.stdout, language="text")
                st.session_state["pipeline_run"] = True

st.divider()

# --- DOWNLOAD SECTION ---
st.subheader("3. Export Insights")
if st.session_state.get('pipeline_run', False):
    if OUTPUT_XLSX_PATH.exists():
        df = pd.read_excel(OUTPUT_XLSX_PATH)
        st.success("Data successfully compiled into Excel format.")
        st.dataframe(df, use_container_width=True)

        excel_data = OUTPUT_XLSX_PATH.read_bytes()
        st.download_button(
            label=f"📥 Download {EXCEL_OUTPUT_FILENAME}",
            data=excel_data,
            file_name=EXCEL_OUTPUT_FILENAME,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
    else:
        st.error(f"Pipeline finished, but output file was not found: {OUTPUT_XLSX_PATH}")
else:
    st.info("👆 Run the pipeline to generate the Excel report.")