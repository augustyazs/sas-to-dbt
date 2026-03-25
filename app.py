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
    render_analyzer_detail,
    render_resolver_detail,
    render_review_detail,
    render_generated_files,
    render_documentation,
    render_sttm,
    render_cost_summary,
)
from ui.runner import run_pipeline
from utils.logger import get_current_run_logs
from config.settings import INPUTS_DIR


# ── HELPERS — defined before use ─────────────────────────────────────────────
def _render_architect_detail(plan):
    with st.expander("Architect Detail", expanded=False):
        tab1, tab2 = st.tabs(["Planned Models", "Edge Cases"])
        with tab1:
            for m in plan.models:
                st.markdown(f"**{m.name}** `{m.layer}` — {m.logic[:80]}")
        with tab2:
            if plan.edge_cases:
                for ec in plan.edge_cases:
                    badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(ec.risk, "⚪")
                    st.markdown(f"{badge} **{ec.pattern}**")
                    st.caption(ec.recommendation)
            else:
                st.success("No edge cases flagged.")


def _render_generator_detail(project):
    with st.expander("Developer (Generator) Detail", expanded=False):
        col1, col2, col3 = st.columns(3)
        col1.metric("Models",        len(project.models))
        col2.metric("Macros",        len(project.macros))
        col3.metric("Not Converted", len(project.not_converted))
        if project.not_converted:
            st.markdown("**Not Converted:**")
            for item in project.not_converted:
                st.markdown(f"- {item}")


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
    .zs-header h1 { font-size: 28px; font-weight: 700; margin: 0; }
    .zs-header p  { font-size: 14px; color: #888; margin: 0; }
    .log-box {
        background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px;
        font-family: monospace; font-size: 12px; max-height: 400px; overflow-y: auto;
        border: 1px solid #334155; white-space: pre-wrap;
    }
    .human-review-alert {
        background: #fef3c7; border: 2px solid #f59e0b; border-radius: 8px;
        padding: 16px; margin: 12px 0;
    }
    .human-review-alert h3 { color: #92400e; margin: 0 0 8px 0; }
    .human-review-alert p  { color: #78350f; margin: 4px 0; font-size: 14px; }
</style>
""", unsafe_allow_html=True)

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


# ── SIDEBAR ───────────────────────────────────────────────────────────────────
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
    **Pipeline Steps:**
    1. **Analyzer** — Parse SAS, extract metadata
    2. **Resolver** — Map on-prem → cloud names
    3. **Architect** — Plan dbt model structure
    4. **Developer** — Generate dbt project
    5. **Reviewer ↔ Fixer** — Validate & fix
    6. **Write Output** — Persist dbt files
    7. **Documenter** — Business documentation
    8. **STTM** — Source-to-target mapping
    """)


# ── INPUT SCANNING ────────────────────────────────────────────────────────────
SAS_DIR     = INPUTS_DIR / "sas_scripts"
MAPPING_DIR = INPUTS_DIR / "column_mapping"

def get_sas_files():
    if SAS_DIR.exists():
        return sorted([f.name for f in SAS_DIR.glob("*.sas")] +
                      [f.name for f in SAS_DIR.glob("*.txt")])
    return []

def get_mapping_files():
    if MAPPING_DIR.exists():
        return sorted([f.name for f in MAPPING_DIR.glob("*.json")] +
                      [f.name for f in MAPPING_DIR.glob("*.csv")])
    return []


# ── INPUTS SECTION ────────────────────────────────────────────────────────────
st.header("📂 Inputs")

sas_code    = None
mapping_raw = None

tab_select, tab_upload = st.tabs(["Select from Git Repo", "Upload Files"])

with tab_select:
    col1, col2 = st.columns(2)
    with col1:
        sas_files = get_sas_files()
        if sas_files:
            selected_sas = st.selectbox(
                "Select SAS Script from Git Repo Inputs folder",
                ["-- select --"] + sas_files, key="sas_select",
            )
            if selected_sas != "-- select --":
                for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                    try:
                        sas_code = (SAS_DIR / selected_sas).read_text(encoding=enc)
                        if sas_code.strip(): break
                    except (UnicodeDecodeError, ValueError):
                        continue
        else:
            st.info("No .sas files found in inputs/sas_scripts/")
    with col2:
        mapping_files = get_mapping_files()
        if mapping_files:
            selected_mapping = st.selectbox(
                "Select Column Mapping", ["-- select --"] + mapping_files, key="map_select",
            )
            if selected_mapping != "-- select --":
                mapping_raw = (MAPPING_DIR / selected_mapping).read_text(encoding="utf-8")
        else:
            st.info("No .json mapping files found in inputs/")

with tab_upload:
    col1, col2 = st.columns(2)
    with col1:
        sas_file = st.file_uploader("Upload SAS Script", type=["sas", "txt"], key="sas_upload")
        if sas_file:
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    sas_code = sas_file.getvalue().decode(enc)
                    if sas_code.strip(): break
                except (UnicodeDecodeError, ValueError):
                    continue
    with col2:
        mapping_file = st.file_uploader("Upload Column Mapping", type=["json", "csv"], key="mapping_upload")
        if mapping_file:
            mapping_raw = mapping_file.getvalue().decode("utf-8")

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

col_run, col_status = st.columns([1, 4])
with col_run:
    run_clicked = st.button(
        "🚀 Run Pipeline", disabled=not can_run,
        type="primary", use_container_width=True,
    )

# ── EVERYTHING BELOW ONLY RUNS WHEN BUTTON IS CLICKED ────────────────────────
# This is identical to the pattern that worked before — no st.stop(), no session
# state gating. The if-block naturally prevents re-execution on re-renders since
# run_clicked is False on every render except the one where the button is pressed.
if run_clicked and can_run:
    try:
        mapping_data = json.loads(mapping_raw)
        col_mappings = [ColumnMapping(**entry) for entry in mapping_data]
    except Exception as e:
        st.error(f"Failed to parse mapping file: {e}")
        st.stop()

    conventions = DbtConventions()

    st.header("⚙️ Pipeline Progress")
    status_container = st.empty()
    step_containers  = render_pipeline_steps()

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

    # ── HUMAN REVIEW ALERTS ───────────────────────────────────────────────────
    st.divider()
    _show_human_review = False

    if final_state.get("resolved_mappings"):
        rm = final_state["resolved_mappings"]
        if rm.unresolved_tables:
            _show_human_review = True
            st.markdown(f"""
            <div class="human-review-alert">
                <h3>👤 Human Review Required — Unresolved Mappings</h3>
                <p><b>The following tables/columns could not be mapped to cloud equivalents:</b></p>
                <p>{'<br>'.join('• ' + t for t in rm.unresolved_tables)}</p>
                <p>Please update the column mapping file with the correct cloud names and re-run.</p>
            </div>
            """, unsafe_allow_html=True)

    if final_state.get("review"):
        review = final_state["review"]
        if not review.is_valid:
            errors = [i for i in review.issues if i.severity == "error"]
            if errors:
                _show_human_review = True
                error_details = "<br>".join(f"• <b>{i.file}</b>: {i.issue}" for i in errors[:10])
                st.markdown(f"""
                <div class="human-review-alert">
                    <h3>👤 Human Review Required — Logic Parity Issues</h3>
                    <p><b>The following errors were not resolved after max retry attempts:</b></p>
                    <p>{error_details}</p>
                    <p>Please review the generated files and correct manually, or provide additional
                    business rules and re-run.</p>
                </div>
                """, unsafe_allow_html=True)

    # ── STEP DETAILS ─────────────────────────────────────────────────────────
    st.divider()
    st.header("🔍 Agents Summary")
    if final_state.get("analysis"):
        render_analyzer_detail(final_state["analysis"])
    if final_state.get("resolved_mappings"):
        render_resolver_detail(final_state["resolved_mappings"])
    if final_state.get("migration_plan"):
        _render_architect_detail(final_state["migration_plan"])
    if final_state.get("dbt_project"):
        _render_generator_detail(final_state["dbt_project"])
    if final_state.get("review"):
        render_review_detail(final_state["review"])

    # ── PIPELINE LOGS ─────────────────────────────────────────────────────────
    st.divider()
    st.header("📝 Pipeline Logs")
    _EXCLUDED = ("ingestion_blocks", "sttm_output", "sas_documentation")
    log_files = [
        f for f in get_current_run_logs()
        if not any(f.stem.startswith(p) for p in _EXCLUDED)
    ]
    if log_files:
        log_tabs = st.tabs([f.stem for f in log_files])
        for tab, log_file in zip(log_tabs, log_files):
            with tab:
                try:
                    content = json.loads(log_file.read_text(encoding="utf-8"))
                    st.markdown(
                        f'<div class="log-box">{json.dumps(content, indent=2)}</div>',
                        unsafe_allow_html=True,
                    )
                except Exception:
                    raw = log_file.read_text(encoding="utf-8")
                    st.markdown(
                        f'<div class="log-box">{raw[:5000]}</div>',
                        unsafe_allow_html=True,
                    )
    else:
        st.info("No logs generated for this run.")

    # ── OUTPUT SUMMARY (dbt files) ────────────────────────────────────────────
    st.divider()
    if final_state.get("dbt_project"):
        render_generated_files(final_state["dbt_project"])

    # ── COST SUMMARY ─────────────────────────────────────────────────────────
    st.divider()
    if cost_data:
        render_cost_summary(cost_data)

    # ── DOCUMENTATION ─────────────────────────────────────────────────────────
    st.divider()
    render_documentation(final_state.get("sas_documentation"))

    # ── STTM ──────────────────────────────────────────────────────────────────
    st.divider()
    render_sttm(final_state.get("sttm_data"))

    # ── FINAL STATUS ──────────────────────────────────────────────────────────
    st.divider()
    final_status = final_state.get("status", "unknown")
    if final_status in ("done", "complete", "complete_with_warnings"):
        st.success(f"✅ Pipeline completed — Status: {final_status}")
    elif final_status == "halted":
        st.error(f"❌ Pipeline halted — {final_state.get('error', 'See step details')}")
    else:
        st.warning(f"⚠️ Pipeline ended — Status: {final_status}")