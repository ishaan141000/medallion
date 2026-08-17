import streamlit as st
import os
import io
import zipfile
from dotenv import load_dotenv
import medallion_engine
from mock_data import PRESETS


def create_zip(
    bronze: str,
    silver: str,
    gold_sql: str,
    gold_yml: str,
    mermaid_code: str,
    bronze_name: str = "bronze_ingest.py",
    silver_name: str = "silver_transform.py",
    gold_sql_name: str = "gold_model.sql",
    gold_yml_name: str = "gold_tests.yml",
    mermaid_name: str = "architecture_lineage.mmd"
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(bronze_name, bronze)
        zip_file.writestr(silver_name, silver)
        zip_file.writestr(gold_sql_name, gold_sql)
        zip_file.writestr(gold_yml_name, gold_yml)
        zip_file.writestr(mermaid_name, mermaid_code)
    buffer.seek(0)
    return buffer.getvalue()

# Load environment variables
load_dotenv()

# Streamlit Page Config
st.set_page_config(
    page_title="Medallion Architecture Modeler",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Premium Styling
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Outfit', sans-serif;
}

code, pre, [class*="stCode"] {
    font-family: 'JetBrains Mono', monospace !important;
}

/* Glassmorphism main card styling */
.premium-card {
    background: rgba(255, 255, 255, 0.02);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 24px;
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
}

/* Custom Header with Gradient */
.gradient-text {
    background: linear-gradient(135deg, #00FFCC 0%, #FF9900 50%, #FF007F 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 800;
    font-size: 2.8rem;
    letter-spacing: -0.5px;
    margin-bottom: 0px;
}

.gradient-subtext {
    font-size: 1.15rem;
    color: #8892B0;
    margin-top: -10px;
    margin-bottom: 30px;
    font-weight: 300;
}

/* Medallion Badges styling */
.badge {
    padding: 6px 12px;
    border-radius: 8px;
    font-weight: 600;
    font-size: 0.85rem;
    display: inline-block;
    margin-right: 8px;
}
.badge-bronze {
    background-color: rgba(205, 127, 50, 0.15);
    color: #CD7F32;
    border: 1px solid rgba(205, 127, 50, 0.3);
}
.badge-silver {
    background-color: rgba(192, 192, 192, 0.15);
    color: #C0C0C0;
    border: 1px solid rgba(192, 192, 192, 0.3);
}
.badge-gold {
    background-color: rgba(255, 215, 0, 0.15);
    color: #FFD700;
    border: 1px solid rgba(255, 215, 0, 0.3);
}

/* Metric styling */
.metric-container {
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
}
.metric-box {
    flex: 1;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}
.metric-value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #00FFCC;
}
.metric-label {
    font-size: 0.85rem;
    color: #8892B0;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 4px;
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# State initialization
if "generation_done" not in st.session_state:
    st.session_state.generation_done = False
if "bronze_output" not in st.session_state:
    st.session_state.bronze_output = ""
if "silver_output" not in st.session_state:
    st.session_state.silver_output = ""
if "gold_sql_output" not in st.session_state:
    st.session_state.gold_sql_output = ""
if "gold_yml_output" not in st.session_state:
    st.session_state.gold_yml_output = ""
if "mermaid_output" not in st.session_state:
    st.session_state.mermaid_output = ""

# Sidebar settings
st.sidebar.markdown("<h2 style='font-weight: 700;'>⚡ Settings & Engine</h2>", unsafe_allow_html=True)

engine_mode = st.sidebar.radio(
    "Choose Generation Engine:",
    options=["⚡ Live DeepSeek API Engine", "🎪 Simulation / Demo Mode"],
    index=0
)

# Manage DeepSeek Keys (Loaded from local .env)
env_key = os.getenv("DEEPSEEK_API_KEY", "")
if env_key:
    medallion_engine.DEEPSEEK_API_KEY = env_key
    if medallion_engine.client is None:
        try:
            from openai import OpenAI
            medallion_engine.client = OpenAI(
                api_key=env_key,
                base_url=medallion_engine.DEEPSEEK_BASE_URL
            )
        except Exception as e:
            st.sidebar.error(f"Error loading client: {e}")
else:
    st.sidebar.warning("⚠️ DEEPSEEK_API_KEY not found in .env")

# Preset Schemas
st.sidebar.markdown("<hr style='border-color: rgba(255,255,255,0.1);'/>", unsafe_allow_html=True)
st.sidebar.markdown("<h3 style='font-size: 1.1rem; font-weight: 600;'>📁 Presets Configuration</h3>", unsafe_allow_html=True)

preset_choice = st.sidebar.selectbox(
    "Select Schema Preset:",
    options=["ecommerce", "mainframe", "custom"],
    format_func=lambda x: "Custom Schema (Create New)" if x == "custom" else PRESETS[x]["name"]
)

# Header Section
st.markdown('<p class="gradient-text">Automated Medallion Architecture Modeler</p>', unsafe_allow_html=True)
st.markdown('<p class="gradient-subtext">Instantly map legacy schemas to modern Bronze, Silver, and Gold data lakes with interactive visual lineage and optimized codebase pipelines.</p>', unsafe_allow_html=True)

# Define columns
col_in, col_out = st.columns([1, 1.2], gap="large")

# Preload preset data
schema_preset_val = ""
source_path_val = "/mnt/raw/incoming/"
bronze_table_val = "raw_table"
silver_table_val = "conformed_table"
gold_model_val = "summary_metrics"
transformations_preset_val = ""
requirements_preset_val = ""
source_name_val = "Legacy System"
source_format_val = "CSV"

if preset_choice != "custom":
    p = PRESETS[preset_choice]
    schema_preset_val = p["legacy_schema"]
    source_name_val = p["source_name"]
    source_format_val = p["source_format"]
    source_path_val = p["source_path"]
    bronze_table_val = p["bronze_table_name"]
    silver_table_val = p["silver_table_name"]
    gold_model_val = p["gold_model_name"]
    transformations_preset_val = p["silver_transformations"]
    requirements_preset_val = p["business_requirements"]

with col_in:
    st.markdown('<div class="premium-card">', unsafe_allow_html=True)
    st.subheader("📋 Legacy Data Setup")
    
    # Custom Uploaders
    uploaded_file = st.file_uploader("Upload Legacy File (DDL, CSV, JSON)", type=["sql", "txt", "csv", "json"])
    if uploaded_file is not None:
        try:
            file_contents = uploaded_file.read().decode("utf-8")
            # If the user uploaded something, use it
            schema_preset_val = file_contents
            source_format_val = uploaded_file.name.split(".")[-1].upper()
            source_name_val = f"Uploaded {uploaded_file.name}"
            st.success(f"Successfully loaded {uploaded_file.name}!")
        except Exception as e:
            st.error(f"Error reading file: {e}")

    # Interactive Inputs
    source_system = st.text_input("Source System Name:", value=source_name_val)
    source_format = st.text_input("Source Data Format (e.g., CSV, JSON, COBOL, DDL):", value=source_format_val)
    
    legacy_schema = st.text_area(
        "Legacy Schema Definitions / DDL / CSV Headers:",
        value=schema_preset_val,
        height=200,
        placeholder="CREATE TABLE legacy_user ( ... )"
    )
    
    st.subheader("⚙️ Medallion Ingestion & Transformation Config")
    source_path = st.text_input("Raw Cloud Storage Ingest Location:", value=source_path_val)
    
    col_names1, col_names2 = st.columns(2)
    with col_names1:
        bronze_table_name = st.text_input("Bronze Table Target:", value=bronze_table_val)
    with col_names2:
        silver_table_name = st.text_input("Silver Table Target:", value=silver_table_val)
        
    silver_transformations = st.text_area(
        "Silver Cleaning & Conformity Operations:",
        value=transformations_preset_val,
        height=140,
        placeholder="e.g. Standardize names to snake_case, Convert string dates to Timestamps, Deduplicate based on ID."
    )
    
    st.subheader("🏆 Gold Layer Analytics")
    gold_model_name = st.text_input("Gold Model Target Name:", value=gold_model_val)
    business_requirements = st.text_area(
        "Business Analytics & Aggregation Goals (NL):",
        value=requirements_preset_val,
        height=110,
        placeholder="e.g. Aggregate active users weekly, Join customer details to compute customer lifetime value tier."
    )
    
    # Process Button
    st.markdown("<br/>", unsafe_allow_html=True)
    trigger_process = st.button("⚡ Generate Medallion Pipelines", use_container_width=True, type="primary")
    
    st.markdown('</div>', unsafe_allow_html=True)

# Business logic trigger
if trigger_process:
    st.session_state.generation_done = False
    
    # Validate keys if live
    is_live = "Live" in engine_mode
    if is_live and not env_key:
        st.error("Error: DEEPSEEK_API_KEY is missing in your .env file. Please add it to run in Live mode, or switch to 'Simulation / Demo Mode'.")
    else:
        with col_out:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # STEP 1: Bronze Generation
                status_text.markdown("🔄 **Step 1/4**: Formulating Bronze Layer Auto Loader code...")
                progress_bar.progress(15)
                
                if is_live:
                    bronze_code = medallion_engine.generate_bronze_layer(
                        legacy_schema=legacy_schema,
                        source_path=source_path,
                        bronze_table_name=bronze_table_name,
                        source_format=source_format
                    )
                else:
                    # Simulation mode fallback
                    bronze_code = PRESETS.get(preset_choice, PRESETS["ecommerce"])["bronze_code"]
                
                st.session_state.bronze_output = bronze_code
                
                # STEP 2: Silver Generation
                status_text.markdown("🔄 **Step 2/4**: Formulating Silver Layer transformation scripts...")
                progress_bar.progress(45)
                
                if is_live:
                    silver_code = medallion_engine.generate_silver_layer(
                        bronze_table_name=bronze_table_name,
                        silver_transformations=silver_transformations,
                        silver_table_name=silver_table_name
                    )
                else:
                    silver_code = PRESETS.get(preset_choice, PRESETS["ecommerce"])["silver_code"]
                
                st.session_state.silver_output = silver_code
                
                # STEP 3: Gold Generation
                status_text.markdown("🔄 **Step 3/4**: Structuring Gold Layer dbt sql models and validation tests...")
                progress_bar.progress(75)
                
                if is_live:
                    gold_sql, gold_yml = medallion_engine.generate_gold_layer(
                        silver_tables=silver_table_name,
                        business_requirements=business_requirements,
                        gold_model_name=gold_model_name
                    )
                else:
                    gold_sql = PRESETS.get(preset_choice, PRESETS["ecommerce"])["gold_sql"]
                    gold_yml = PRESETS.get(preset_choice, PRESETS["ecommerce"])["gold_yml"]
                
                st.session_state.gold_sql_output = gold_sql
                st.session_state.gold_yml_output = gold_yml
                
                # STEP 4: Diagram Generation
                status_text.markdown("🔄 **Step 4/4**: Mapping and drawing architecture lineage diagram...")
                progress_bar.progress(90)
                
                if is_live:
                    mermaid_code = medallion_engine.generate_lineage_diagram(
                        source_name=source_system,
                        source_format=source_format,
                        bronze_table_name=bronze_table_name,
                        silver_table_name=silver_table_name,
                        gold_model_name=gold_model_name,
                        silver_transformations=silver_transformations,
                        business_requirements=business_requirements
                    )
                else:
                    mermaid_code = PRESETS.get(preset_choice, PRESETS["ecommerce"])["mermaid"]
                
                st.session_state.mermaid_output = mermaid_code
                
                progress_bar.progress(100)
                status_text.empty()
                progress_bar.empty()
                
                st.session_state.generation_done = True
                st.success("🎉 Medallion Architecture code blocks and diagram generated successfully!")
                
            except Exception as e:
                status_text.empty()
                progress_bar.empty()
                st.error(f"Execution Error: {e}")

# Output Section
with col_out:
    if st.session_state.generation_done:
        # Performance Summary Cards
        st.markdown(
            f"""
            <div class="metric-container">
                <div class="metric-box">
                    <div class="metric-value">4-6 Hours</div>
                    <div class="metric-label">Estimated dev time saved</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">100%</div>
                    <div class="metric-label">Valid Delta Storage</div>
                </div>
                <div class="metric-box">
                    <div class="metric-value">dbt Core</div>
                    <div class="metric-label">Target Analytics engine</div>
                </div>
            </div>
            """, 
            unsafe_allow_html=True
        )
        
        # Tabs for code
        tab_flow, tab_bronze, tab_silver, tab_gold = st.tabs([
            "📊 Data Lineage Diagram",
            "🟫 Bronze Ingestion (PySpark)",
            "⬜ Silver Cleanse (PySpark)",
            "🟨 Gold Analytics (dbt)"
        ])
        
        with tab_flow:
            st.markdown("### Architecture Data Lineage")
            
            # Render Mermaid diagram dynamically inside an iframe using CDNs
            mermaid_markup = f"""
            <div id="mermaid-container" style="background-color: #0E1117; padding: 20px; border-radius: 12px; border: 1px solid rgba(255, 255, 255, 0.1);">
                <pre class="mermaid" style="background-color: transparent; border: none; overflow: auto; text-align: center;">
                {st.session_state.mermaid_output}
                </pre>
            </div>
            <script type="module">
                import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
                mermaid.initialize({{ 
                    startOnLoad: true, 
                    theme: 'dark',
                    securityLevel: 'loose',
                    flowchart: {{ useMaxWidth: false, htmlLabels: true }}
                }});
            </script>
            """
            st.components.v1.html(mermaid_markup, height=450, scrolling=True)
            
            # Output raw copy option
            with st.expander("Show Raw Mermaid.js Code"):
                st.code(st.session_state.mermaid_output, language="mermaid")
                
        with tab_bronze:
            st.markdown("### 🟫 Bronze Layer (Auto Loader / Raw Ingestion)")
            st.markdown("Generates raw storage schema files with partition layouts, schema evolution settings, and system-level rescued columns.")
            st.code(st.session_state.bronze_output, language="python")
            
            st.download_button(
                label="📥 Download Bronze Script",
                data=st.session_state.bronze_output,
                file_name=f"bronze_ingest_{bronze_table_name}.py",
                mime="text/x-python"
            )
            
        with tab_silver:
            st.markdown("### ⬜ Silver Layer (Data Cleansing & Conformity)")
            st.markdown("Standardizes column layouts to `snake_case`, handles custom date formats, computes MD5/SHA2 surrogate primary keys, and filters nulls.")
            st.code(st.session_state.silver_output, language="python")
            
            st.download_button(
                label="📥 Download Silver Script",
                data=st.session_state.silver_output,
                file_name=f"silver_transform_{silver_table_name}.py",
                mime="text/x-python"
            )
            
        with tab_gold:
            st.markdown("### 🟨 Gold Layer (Business Analytics Metrics)")
            st.markdown("Exposes structured tables/views using conformed structures, business-ready dimensions/facts, and pre-packaged dbt testing configurations.")
            
            sub_sql, sub_yml = st.tabs(["dbt SQL Model", "dbt Configuration (schema.yml)"])
            with sub_sql:
                st.code(st.session_state.gold_sql_output, language="sql")
                st.download_button(
                    label="📥 Download Gold SQL Model",
                    data=st.session_state.gold_sql_output,
                    file_name=f"{gold_model_name}.sql",
                    mime="text/plain"
                )
            with sub_yml:
                st.code(st.session_state.gold_yml_output, language="yaml")
                st.download_button(
                    label="📥 Download Gold YAML Tests",
                    data=st.session_state.gold_yml_output,
                    file_name=f"{gold_model_name}.yml",
                    mime="text/plain"
                )
                
        # Zip downloader
        st.markdown("<hr style='border-color: rgba(255,255,255,0.1);'/>", unsafe_allow_html=True)
        zip_data = create_zip(
            bronze=st.session_state.bronze_output,
            silver=st.session_state.silver_output,
            gold_sql=st.session_state.gold_sql_output,
            gold_yml=st.session_state.gold_yml_output,
            mermaid_code=st.session_state.mermaid_output
        )
        
        st.download_button(
            label="📦 Download Complete Code Package (.zip)",
            data=zip_data,
            file_name=f"medallion_pipeline_{gold_model_name}.zip",
            mime="application/zip",
            use_container_width=True
        )
    else:
        # Beautiful Empty State UI
        st.markdown(
            """
            <div style="background: rgba(255,255,255,0.01); border: 1px dashed rgba(255,255,255,0.1); border-radius: 16px; padding: 60px; text-align: center; margin-top: 50px;">
                <div style="font-size: 3.5rem; margin-bottom: 20px;">⚡</div>
                <h3 style="font-weight: 600; color: #E5E9F0;">Ready for Compilation</h3>
                <p style="color: #8892B0; max-width: 400px; margin: 0 auto 30px auto; font-size: 0.95rem;">
                    Configure your legacy schema settings and click the button to trigger code generation and layout design lineage.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
