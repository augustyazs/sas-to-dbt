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
    render_pipeline_progress,
)
from ui.runner import run_pipeline
from utils.logger import get_current_run_logs
from config.settings import INPUTS_DIR


# ── SESSION STATE INIT ────────────────────────────────────────────────────────
if "api_key_input" not in st.session_state:
    st.session_state.api_key_input = ""
if "final_state" not in st.session_state:
    st.session_state.final_state = None
if "cost_data" not in st.session_state:
    st.session_state.cost_data = None
if "run_error" not in st.session_state:
    st.session_state.run_error = None
if "log_files" not in st.session_state:
    st.session_state.log_files = []
if "pipeline_steps" not in st.session_state:
    st.session_state.pipeline_steps = []
if "write_output_status" not in st.session_state:
    st.session_state.write_output_status = "pending"
if "pipeline_triggered" not in st.session_state:
    st.session_state.pipeline_triggered = False


# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
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
        background: #f8f9fa; padding: 12px;
        border-radius: 8px; border: 1px solid #e0e0e0;
    }
    div[data-testid="stMetric"] label { color: #333 !important; }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1a1a1a !important; font-weight: 700;
    }
    div[data-testid="stExpander"] { border: 1px solid #334155; border-radius: 8px; }

    .zs-header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }
    .zs-header img { height: 40px; }
    .zs-header h1  { font-size: 28px; font-weight: 700; margin: 0; }
    .zs-header p   { font-size: 14px; color: #888; margin: 0; }

    .log-box {
        background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px;
        font-family: monospace; font-size: 12px; max-height: 400px; overflow-y: auto;
        border: 1px solid #334155; white-space: pre-wrap;
    }

    /* Full-height vertical divider between output and progress columns */
    .col-divider {
        border-left: 1px solid #1e293b;
        height: 100%;
        min-height: 800px;
        margin: 0;
    }

    /* Output placeholder inside dropdowns */
    .output-waiting {
        padding: 14px 16px;
        background: #0f172a;
        border: 1px dashed #1e3a5f;
        border-radius: 6px;
        color: #3b82f6;
        font-size: 13px;
        text-align: center;
        margin: 4px 0;
    }

    /* Status footer */
    .status-bar {
        margin-top: 24px; padding: 12px 20px; border-radius: 8px;
        font-size: 14px; font-weight: 600; text-align: center;
        border-width: 1px; border-style: solid;
    }
    .status-bar.done  { background:#052e16; border-color:#16a34a; color:#4ade80; }
    .status-bar.warn  { background:#431407; border-color:#ea580c; color:#fb923c; }
    .status-bar.error { background:#450a0a; border-color:#dc2626; color:#f87171; }
</style>
""", unsafe_allow_html=True)


# ── HELPERS ───────────────────────────────────────────────────────────────────
logo_path = Path(__file__).parent / "assests" / "zs_logo.png"
ZS_LOGO_URL = ""
if logo_path.exists():
    ZS_LOGO_URL = f"data:image/png;base64,{base64.b64encode(logo_path.read_bytes()).decode()}"


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
    with st.expander("Generator Detail", expanded=False):
        col1, col2, col3 = st.columns(3)
        col1.metric("Models",        len(project.models))
        col2.metric("Macros",        len(project.macros))
        col3.metric("Not Converted", len(project.not_converted))
        if project.not_converted:
            st.markdown("**Not Converted:**")
            for item in project.not_converted:
                st.markdown(f"- {item}")


def _render_fixer_detail(dbt_project):
    if dbt_project is None:
        return
    with st.expander("Fixer Detail", expanded=False):
        col1, col2 = st.columns(2)
        col1.metric("Models in project", len(dbt_project.models))
        col2.metric("Macros in project", len(dbt_project.macros))
        if dbt_project.not_converted:
            st.markdown("**Not converted after fixing:**")
            for item in dbt_project.not_converted:
                st.markdown(f"- {item}")


def _waiting_placeholder(label: str):
    st.markdown(
        f'<div class="output-waiting">⏳ &nbsp; {label} — pipeline in progress…</div>',
        unsafe_allow_html=True,
    )


def _run_pipeline_and_store(sas_code, mapping_raw, progress_slot):
    st.session_state.run_error   = None
    st.session_state.final_state = None
    st.session_state.cost_data   = None
    st.session_state.log_files   = []
    st.session_state.pipeline_steps = []
    st.session_state.write_output_status = "pending"

    try:
        col_mappings = [ColumnMapping(**e) for e in json.loads(mapping_raw)]
    except Exception as e:
        st.session_state.run_error = f"Failed to parse mapping file: {e}"
        return

    conventions = DbtConventions()

    with st.spinner("Running pipeline…"):
        final_state, cost_data = run_pipeline(
            sas_code=sas_code,
            mappings=col_mappings,
            conventions=conventions,
            progress_slot=progress_slot,
        )

    if final_state is None:
        st.session_state.run_error = "Pipeline failed. Check logs for details."
        return

    st.session_state.final_state = final_state
    st.session_state.cost_data   = cost_data
    st.session_state.log_files   = get_current_run_logs()
    st.rerun()


# ── LEFT SIDEBAR ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")

    st.subheader("API Key")
    api_key_input = st.text_input(
        "OpenAI API Key",
        type="password",
        placeholder="sk-...",
        help="Not stored anywhere. Lives only in your browser session.",
        value=st.session_state.api_key_input,
    )
    st.session_state.api_key_input = api_key_input
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
    4. **Generator** — Generate dbt project
    5. **Reviewer ↔ Fixer** — Validate & fix
    6. **Write Output** — Persist dbt files
    7. **Documenter** — Business documentation
    8. **STTM** — Source-to-target mapping
    """)


# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="zs-header">
    <img src="{ZS_LOGO_URL}" alt="ZS Logo">
    <div>
        <h1>HPP Capabilities</h1>
        <p>SAS to dbt Agentic Code Migration</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── INPUTS ────────────────────────────────────────────────────────────────────
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

st.header("📂 Inputs")
sas_code    = None
mapping_raw = None

tab_select, tab_upload = st.tabs(["Select from Git Repo", "Upload Files"])
with tab_select:
    c1, c2 = st.columns(2)
    with c1:
        sas_files = get_sas_files()
        if sas_files:
            sel = st.selectbox("Select SAS Script", ["-- select --"] + sas_files, key="sas_select")
            if sel != "-- select --":
                for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                    try:
                        sas_code = (SAS_DIR / sel).read_text(encoding=enc)
                        if sas_code.strip(): break
                    except (UnicodeDecodeError, ValueError):
                        continue
        else:
            st.info("No .sas files found in inputs/sas_scripts/")
    with c2:
        map_files = get_mapping_files()
        if map_files:
            sel = st.selectbox("Select Column Mapping", ["-- select --"] + map_files, key="map_select")
            if sel != "-- select --":
                mapping_raw = (MAPPING_DIR / sel).read_text(encoding="utf-8")
        else:
            st.info("No .json mapping files found in inputs/")

with tab_upload:
    c1, c2 = st.columns(2)
    with c1:
        up = st.file_uploader("Upload SAS Script", type=["sas", "txt"], key="sas_upload")
        if up:
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    sas_code = up.getvalue().decode(enc)
                    if sas_code.strip(): break
                except (UnicodeDecodeError, ValueError):
                    continue
    with c2:
        up = st.file_uploader("Upload Column Mapping", type=["json", "csv"], key="mapping_upload")
        if up:
            mapping_raw = up.getvalue().decode("utf-8")

if sas_code:    render_sas_preview(sas_code)
if mapping_raw: render_mapping_preview(mapping_raw)

# ── RUN BUTTON ────────────────────────────────────────────────────────────────
can_run = sas_code is not None and mapping_raw is not None and bool(st.session_state.api_key_input)
if not can_run:
    missing = []
    if not sas_code:                       missing.append("SAS script")
    if not mapping_raw:                    missing.append("column mapping")
    if not st.session_state.api_key_input: missing.append("API key (sidebar)")
    st.info(f"Missing: {', '.join(missing)}")

col_run, _ = st.columns([1, 4])
with col_run:
    run_clicked = st.button(
        "🚀 Run Pipeline", disabled=not can_run,
        type="primary", use_container_width=True,
    )

if run_clicked and can_run:
    st.session_state.pipeline_triggered = True

st.divider()

# ── MAIN AREA: outputs [4] | thin divider [0.02] | pipeline progress [1] ─────
col_main, col_div, col_progress = st.columns([4, 0.02, 1])

with col_div:
    st.markdown('<div class="col-divider"></div>', unsafe_allow_html=True)

with col_progress:
    progress_slot = st.empty()
    render_pipeline_progress(
        progress_slot,
        st.session_state.pipeline_steps,
        st.session_state.get("write_output_status", "pending"),
    )

# ── LEFT: outputs ─────────────────────────────────────────────────────────────
final_state = st.session_state.final_state
cost_data   = st.session_state.cost_data
log_files   = st.session_state.log_files
triggered   = st.session_state.pipeline_triggered

with col_main:
    if st.session_state.run_error:
        st.error(st.session_state.run_error)

    if triggered:
        pipeline_done = final_state is not None

        # 1. Step Details
        with st.expander("Step Details", expanded=False):
            if pipeline_done and final_state.get("analysis"):
                render_analyzer_detail(final_state["analysis"])
                if final_state.get("resolved_mappings"):
                    render_resolver_detail(final_state["resolved_mappings"])
                if final_state.get("migration_plan"):
                    _render_architect_detail(final_state["migration_plan"])
                if final_state.get("dbt_project"):
                    _render_generator_detail(final_state["dbt_project"])
                if final_state.get("review"):
                    render_review_detail(final_state["review"])
                if final_state.get("dbt_project"):
                    _render_fixer_detail(final_state["dbt_project"])
            else:
                _waiting_placeholder("Agent step details")

        # 2. Pipeline Logs
        with st.expander("Pipeline Logs", expanded=False):
            _EXCLUDED = ("ingestion_blocks", "sttm_output", "sas_documentation")
            visible_logs = [
                f for f in log_files
                if not any(f.stem.startswith(p) for p in _EXCLUDED)
            ]
            if visible_logs:
                log_tabs = st.tabs([f.stem for f in visible_logs])
                for tab, log_file in zip(log_tabs, visible_logs):
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
                _waiting_placeholder("Pipeline logs")

        # 3. Output Summary
        with st.expander("Output Summary", expanded=False):
            if pipeline_done and final_state.get("dbt_project"):
                render_generated_files(final_state["dbt_project"])
            else:
                _waiting_placeholder("dbt output files")

        # 4. Cost Summary
        with st.expander("Cost Summary", expanded=False):
            if pipeline_done and cost_data:
                render_cost_summary(cost_data)
            else:
                _waiting_placeholder("Cost breakdown")

        # 5. Documentation
        with st.expander("Documentation", expanded=False):
            if pipeline_done and final_state.get("sas_documentation"):
                render_documentation(final_state["sas_documentation"])
            else:
                _waiting_placeholder("Pipeline documentation")

        # 6. STTM
        with st.expander("Source-to-Target Mapping", expanded=False):
            if pipeline_done and final_state.get("sttm_data"):
                render_sttm(final_state["sttm_data"])
            else:
                _waiting_placeholder("Source-to-target mapping")

        # Status footer
        if pipeline_done:
            final_status = final_state.get("status", "unknown")
            if final_status in ("done", "complete", "complete_with_warnings"):
                st.markdown(
                    '<div class="status-bar done">✅ &nbsp; Pipeline run finished successfully</div>',
                    unsafe_allow_html=True,
                )
            elif final_status == "halted":
                st.markdown(
                    f'<div class="status-bar error">❌ &nbsp; Pipeline halted — {final_state.get("error", "See logs")}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="status-bar warn">⚠️ &nbsp; Pipeline ended — Status: {final_status}</div>',
                    unsafe_allow_html=True,
                )

# ── Run pipeline AFTER layout is rendered ────────────────────────────────────
if run_clicked and can_run:
    _run_pipeline_and_store(sas_code, mapping_raw, progress_slot)