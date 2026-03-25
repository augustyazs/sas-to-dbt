import json
import os
import streamlit as st
import base64
from pathlib import Path
from models.schemas import ColumnMapping, DbtConventions
from ui.components import (
    render_sas_preview,
    render_mapping_preview,
    render_pipeline_steps,
    render_output_sections,
    render_agents_summary,
    render_pipeline_logs,
    render_output_summary,
    render_documentation,
    render_sttm,
    render_cost_summary,
)
from ui.runner import run_pipeline
from utils.logger import get_current_run_logs
from config.settings import INPUTS_DIR


st.set_page_config(
    page_title="HPP Capabilities — SAS to dbt",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }

    div[data-testid="stMetric"] {
        background: #f8f9fa;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
    }
    div[data-testid="stMetric"] label { color: #333 !important; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1a1a1a !important;
        font-weight: 700;
    }
    div[data-testid="stExpander"] { border: 1px solid #334155; border-radius: 8px; }

    .zs-header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }
    .zs-header img { height: 40px; }
    .zs-header h1  { font-size: 28px; font-weight: 700; margin: 0; }
    .zs-header p   { font-size: 14px; color: #888; margin: 0; }

    .human-review-alert {
        background: #fef3c7; border: 2px solid #f59e0b; border-radius: 8px;
        padding: 16px; margin: 12px 0;
    }
    .human-review-alert h3 { color: #92400e; margin: 0 0 8px 0; }
    .human-review-alert p  { color: #78350f; margin: 4px 0; font-size: 14px; }

    .status-footer {
        position: fixed; bottom: 0; left: 0; right: 0;
        background: #0f172a; color: #94a3b8;
        padding: 8px 24px; font-size: 13px;
        border-top: 1px solid #1e293b;
        display: flex; align-items: center; gap: 12px;
        z-index: 999;
    }
</style>
""", unsafe_allow_html=True)

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path = Path(__file__).parent / "assests" / "zs_logo.png"
ZS_LOGO_URL = ""
if logo_path.exists():
    ZS_LOGO_URL = f"data:image/png;base64,{base64.b64encode(logo_path.read_bytes()).decode()}"

st.markdown(f"""
<div class="zs-header">
    <img src="{ZS_LOGO_URL}" alt="ZS Logo">
    <div>
        <h1>HPP Capabilities</h1>
        <p>SAS to dbt Agentic Code Migration</p>
    </div>
</div>
""", unsafe_allow_html=True)


# ── LEFT SIDEBAR: config ──────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")

    st.subheader("API Key")
    api_key_input = st.text_input(
        "OpenAI API Key", type="password", placeholder="sk-...",
        help="Not stored anywhere. Lives only in your browser session.",
    )
    if api_key_input:
        os.environ["OPENAI_API_KEY"] = api_key_input

    st.divider()
    st.subheader("Source Control")
    source_platform = st.selectbox("Platform", ["GitHub", "Bitbucket"], key="source_platform")
    repo_name = st.selectbox("Repository", [
        "hpp-analytics/sas-to-dbt",
        "hpp-analytics/dx-data-pipeline",
        "hpp-analytics/rx-etl",
    ], key="repo_name")
    folder_name = st.selectbox("Input Folder", [
        "sas_scripts_input_20260301",
        "sas_scripts_input_20260215",
        "sas_scripts_input_20260128",
    ], key="folder_name")
    st.caption(f"📁 {source_platform} / {repo_name} / {folder_name}")

    st.divider()
    st.subheader("Output")
    output_platform = st.selectbox("Platform", ["GitHub", "Bitbucket"], key="output_platform")
    output_repo = st.selectbox("Repository", [
        "hpp-analytics/dbt-pharmacy-models",
        "hpp-analytics/dbt-rx-warehouse",
        "hpp-analytics/sas-to-dbt",
    ], key="output_repo")
    output_branch = st.selectbox("Branch",
        ["feature/sas-migration", "develop", "main"], key="output_branch")
    st.caption(f"📦 {output_platform} / {output_repo} / {output_branch}")

    st.divider()
    st.subheader("About")
    st.markdown("""
    **Pipeline:**
    1. Analyzer
    2. Resolver
    3. Architect
    4. Developer
    5. Reviewer ↔ Fixer
    6. Write Output
    7. Documenter
    8. STTM Generator
    """)


# ── INPUT FILES ───────────────────────────────────────────────────────────────
SAS_DIR     = INPUTS_DIR / "sas_scripts"
MAPPING_DIR = INPUTS_DIR / "column_mapping"

def _get_sas_files():
    if SAS_DIR.exists():
        return sorted([f.name for f in SAS_DIR.glob("*.sas")] + [f.name for f in SAS_DIR.glob("*.txt")])
    return []

def _get_mapping_files():
    if MAPPING_DIR.exists():
        return sorted([f.name for f in MAPPING_DIR.glob("*.json")] + [f.name for f in MAPPING_DIR.glob("*.csv")])
    return []


st.header("📂 Inputs")
sas_code    = None
mapping_raw = None

tab_select, tab_upload = st.tabs(["Select from Git Repo", "Upload Files"])

with tab_select:
    c1, c2 = st.columns(2)
    with c1:
        sas_files = _get_sas_files()
        if sas_files:
            sel_sas = st.selectbox("Select SAS Script", ["-- select --"] + sas_files, key="sas_select")
            if sel_sas != "-- select --":
                for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                    try:
                        sas_code = (SAS_DIR / sel_sas).read_text(encoding=enc)
                        if sas_code.strip(): break
                    except (UnicodeDecodeError, ValueError):
                        continue
        else:
            st.info("No .sas files found in inputs/sas_scripts/")
    with c2:
        map_files = _get_mapping_files()
        if map_files:
            sel_map = st.selectbox("Select Column Mapping", ["-- select --"] + map_files, key="map_select")
            if sel_map != "-- select --":
                mapping_raw = (MAPPING_DIR / sel_map).read_text(encoding="utf-8")
        else:
            st.info("No .json mapping files found in inputs/")

with tab_upload:
    c1, c2 = st.columns(2)
    with c1:
        up_sas = st.file_uploader("Upload SAS Script", type=["sas", "txt"], key="sas_upload")
        if up_sas:
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    sas_code = up_sas.getvalue().decode(enc)
                    if sas_code.strip(): break
                except (UnicodeDecodeError, ValueError):
                    continue
    with c2:
        up_map = st.file_uploader("Upload Column Mapping", type=["json", "csv"], key="mapping_upload")
        if up_map:
            mapping_raw = up_map.getvalue().decode("utf-8")

if sas_code:    render_sas_preview(sas_code)
if mapping_raw: render_mapping_preview(mapping_raw)


# ── RUN BUTTON ────────────────────────────────────────────────────────────────
can_run = sas_code is not None and mapping_raw is not None and bool(api_key_input)

if not can_run:
    missing = []
    if not sas_code:      missing.append("SAS script")
    if not mapping_raw:   missing.append("column mapping")
    if not api_key_input: missing.append("API key (sidebar)")
    st.info(f"Missing: {', '.join(missing)}")

run_clicked = st.button(
    "🚀 Run Pipeline", disabled=not can_run,
    type="primary", use_container_width=False,
)

if not run_clicked:
    st.stop()

# ── PARSE INPUTS ──────────────────────────────────────────────────────────────
try:
    col_mappings = [ColumnMapping(**e) for e in json.loads(mapping_raw)]
except Exception as e:
    st.error(f"Failed to parse mapping file: {e}")
    st.stop()

conventions = DbtConventions()

# ── TWO-COLUMN LAYOUT: center = outputs, right = pipeline progress ────────────
st.divider()
col_main, col_progress = st.columns([3, 1])

with col_progress:
    st.markdown("### ⚙️ Pipeline Progress")
    timeline_containers, loop_area = render_pipeline_steps()

with col_main:
    st.markdown("### 📋 Pipeline Outputs")
    output_sections = render_output_sections()

# ── RUN ───────────────────────────────────────────────────────────────────────
final_state, cost_data = run_pipeline(
    sas_code=sas_code,
    mappings=col_mappings,
    conventions=conventions,
    timeline_containers=timeline_containers,
    loop_area=loop_area,
)

if final_state is None:
    st.error("Pipeline failed. Check logs for details.")
    st.stop()

# ── FILL OUTPUT SECTIONS ──────────────────────────────────────────────────────
log_files    = get_current_run_logs()
final_status = final_state.get("status", "unknown")

# Human review alerts (only if issues)
if final_state.get("resolved_mappings"):
    rm = final_state["resolved_mappings"]
    if rm.unresolved_tables:
        with col_main:
            st.markdown(f"""
            <div class="human-review-alert">
                <h3>👤 Human Review Required — Unresolved Mappings</h3>
                <p><b>Tables that could not be mapped:</b></p>
                <p>{'<br>'.join('• ' + t for t in rm.unresolved_tables)}</p>
                <p>Update the column mapping file with the correct cloud names and re-run.</p>
            </div>
            """, unsafe_allow_html=True)

if final_state.get("review"):
    review = final_state["review"]
    if not review.is_valid:
        errors = [i for i in review.issues if i.severity == "error"]
        if errors:
            with col_main:
                error_details = "<br>".join(f"• <b>{i.file}</b>: {i.issue}" for i in errors[:10])
                st.markdown(f"""
                <div class="human-review-alert">
                    <h3>👤 Human Review Required — Logic Parity Issues</h3>
                    <p><b>Errors not resolved after max retry attempts:</b></p>
                    <p>{error_details}</p>
                </div>
                """, unsafe_allow_html=True)

# Populate each output section
with col_main:

    with output_sections["agents_summary"]:
        render_agents_summary(final_state)

    with output_sections["pipeline_logs"]:
        render_pipeline_logs(log_files)

    with output_sections["output_summary"]:
        if final_state.get("dbt_project"):
            render_output_summary(final_state["dbt_project"])
        else:
            st.info("No dbt project generated.")

    with output_sections["cost_summary"]:
        if cost_data:
            render_cost_summary(cost_data)
        else:
            st.info("No cost data available.")

    with output_sections["documentation"]:
        render_documentation(final_state.get("sas_documentation"))

    with output_sections["sttm"]:
        render_sttm(final_state.get("sttm_data"))


# ── STATUS FOOTER ─────────────────────────────────────────────────────────────
status_icon  = "✅" if final_status in ("done", "complete", "complete_with_warnings") else "❌" if final_status == "halted" else "⚠️"
status_label = {
    "done":                   "Pipeline completed successfully",
    "complete":               "Pipeline completed successfully",
    "complete_with_warnings": "Pipeline completed with warnings",
    "halted":                 f"Pipeline halted — {final_state.get('error', 'see logs')}",
}.get(final_status, f"Pipeline ended — status: {final_status}")

st.markdown(
    f'<div class="status-footer">'
    f'<span>{status_icon}</span>'
    f'<span><b>Status</b> — {status_label}</span>'
    f'</div>',
    unsafe_allow_html=True,
)