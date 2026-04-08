import streamlit as st
import pandas as pd
import time
from io import BytesIO

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Qiagen Intel Pipeline", page_icon="🧬", layout="wide")

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
            st.write("🔍 Searching for new SEC Filings...")
            time.sleep(1.5)
            st.write("🌐 Scraping global competitor news...")
            time.sleep(1.5)
            st.write("🧠 Running GPT-4o-mini extraction on 60+ documents...")
            time.sleep(2)
            status.update(label="Extraction Complete!", state="complete", expanded=False)
        st.session_state['pipeline_run'] = True

st.divider()

# --- DOWNLOAD SECTION ---
st.subheader("3. Export Insights")
if st.session_state.get('pipeline_run', False):
    st.success("Data successfully compiled into Excel format.")
    dummy_data = {
        "Company_Name": ["Thermo Fisher", "Danaher"],
        "R_and_D_Focus": ["Expanding bioprocessing.", "Investing in genomic medicine."],
        "APAC_Strategy": ["New CDMO in Singapore.", "Partnering with Japanese biotech."],
        "AAV_and_LV_Capabilities": ["Scaling AAV production.", "New LV vectors."],
        "Gene_Therapy_Focus": ["Broad platform.", "CRISPR technologies."],
        "MSC_Capabilities": ["Not Mentioned", "Stem cell research division."]
    }
    df = pd.DataFrame(dummy_data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Insights')
    excel_data = output.getvalue()
    
    st.download_button(
        label="📥 Download Competitor_Insights_Final.xlsx",
        data=excel_data,
        file_name="Competitor_Insights_Final.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )
else:
    st.info("👆 Run the pipeline to generate the Excel report.")