import json
import streamlit as st
import base64
from pathlib import Path
from models.schemas import ColumnMapping, DbtConventions
from ui.components import (
    render_file_uploaders,
    render_sas_preview,
    render_mapping_preview,
    render_pipeline_steps,
    render_analyzer_detail,
    render_resolver_detail,
    render_review_detail,
    render_generated_files,
    render_cost_summary,
)
from ui.runner import run_pipeline



st.set_page_config(
    page_title="HPP Capabilities — SAS to dbt",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .stMetric { background: #1e293b; padding: 12px; border-radius: 8px; }
    div[data-testid="stExpander"] { border: 1px solid #334155; border-radius: 8px; }
    .zs-header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }
    .zs-header img { height: 40px; }
    .zs-header h1 { font-size: 28px; font-weight: 700; margin: 0; }
    .zs-header p { font-size: 14px; color: #888; margin: 0; }
</style>
""", unsafe_allow_html=True)

logo_path = Path(__file__).parent / "assests" / "zs_logo.png"
if logo_path.exists():
    logo_b64 = base64.b64encode(logo_path.read_bytes()).decode()
    ZS_LOGO_URL = f"data:image/png;base64,{logo_b64}"

st.markdown(f"""
<div class="zs-header">
    <img src="{ZS_LOGO_URL}" alt="ZS Logo">
    <div>
        <h1>HPP Capabilities</h1>
        <p>SAS → dbt Automated Code Migration • LLM-Powered Agentic Pipeline</p>
    </div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Configuration")

    st.subheader("API Key")
    api_key_input = st.text_input(
        "OpenAI API Key",
        type="password",
        placeholder="sk-...",
        help="Not stored anywhere. Lives only in your browser session.",
    )
    if api_key_input:
        import os
        os.environ["OPENAI_API_KEY"] = api_key_input

    st.subheader("dbt Conventions")
    target_dialect = st.selectbox("Target Dialect", ["postgres_redshift", "redshift", "postgres"], index=0)
    mat_staging = st.selectbox("Staging Materialization", ["view", "table", "ephemeral"], index=0)
    mat_intermediate = st.selectbox("Intermediate Materialization", ["table", "view", "ephemeral"], index=0)
    mat_marts = st.selectbox("Marts Materialization", ["table", "view"], index=0)
    max_joins = st.slider("Max Joins per Model", 4, 15, 8)

    conventions = DbtConventions(
        target_dialect=target_dialect,
        materialization_staging=mat_staging,
        materialization_intermediate=mat_intermediate,
        materialization_marts=mat_marts,
        max_joins_per_model=max_joins,
    )

    st.divider()
    st.subheader("About")
    st.markdown("""
    **Pipeline Steps:**
    1. **Analyzer** — Parse SAS, extract metadata
    2. **Resolver** — Map on-prem → cloud names
    3. **Generator** — Produce dbt project
    4. **Reviewer** — Validate logic parity
    """)


st.header("📂 Inputs")
sas_code, mapping_raw = render_file_uploaders()

if sas_code:
    render_sas_preview(sas_code)
if mapping_raw:
    render_mapping_preview(mapping_raw)

can_run = sas_code is not None and mapping_raw is not None and bool(api_key_input)

if not can_run:
    missing = []
    if not sas_code:
        missing.append("SAS script")
    if not mapping_raw:
        missing.append("column mapping")
    if not api_key_input:
        missing.append("API key (sidebar)")
    st.info(f"Missing: {', '.join(missing)}")

col_run, col_status = st.columns([1, 4])
with col_run:
    run_clicked = st.button("🚀 Run Pipeline", disabled=not can_run, type="primary", use_container_width=True)

if run_clicked and can_run:
    try:
        mapping_data = json.loads(mapping_raw)
        col_mappings = [ColumnMapping(**entry) for entry in mapping_data]
    except (json.JSONDecodeError, Exception) as e:
        st.error(f"Failed to parse mapping file: {e}")
        st.stop()

    st.header("⚙️ Pipeline Progress")
    status_container = st.empty()
    step_containers = render_pipeline_steps()

    with st.spinner("Running pipeline..."):
        final_state, cost_data = run_pipeline(
            sas_code=sas_code,
            mappings=col_mappings,
            conventions=conventions,
            status_container=status_container,
            step_containers=step_containers,
        )

    if final_state is None:
        st.error("Pipeline failed. Check logs for details.")
        st.stop()

    st.divider()

    st.header("🔍 Step Details")
    if final_state.get("analysis"):
        render_analyzer_detail(final_state["analysis"])
    if final_state.get("resolved_mappings"):
        render_resolver_detail(final_state["resolved_mappings"])
    if final_state.get("review"):
        render_review_detail(final_state["review"])

    st.divider()

    if final_state.get("dbt_project"):
        render_generated_files(final_state["dbt_project"])

    st.divider()

    if cost_data:
        render_cost_summary(cost_data)

    final_status = final_state.get("status", "unknown")
    if final_status in ("done", "complete", "complete_with_warnings"):
        st.success(f"✅ Pipeline completed — Status: {final_status}")
    elif final_status == "halted":
        st.error(f"❌ Pipeline halted — {final_state.get('error', 'See step details')}")
    else:
        st.warning(f"⚠️ Pipeline ended — Status: {final_status}")
