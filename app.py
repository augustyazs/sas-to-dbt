import json
import os
import streamlit as st
import base64
from pathlib import Path
from models.schemas import ColumnMapping, DbtConventions
from ui.components import (
    render_sas_preview,
    render_mapping_preview,
    render_pipeline_timeline,
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


# ── SESSION STATE INIT ────────────────────────────────────────────────────────
if "api_key_input"    not in st.session_state: st.session_state.api_key_input    = ""
if "final_state"      not in st.session_state: st.session_state.final_state      = None
if "cost_data"        not in st.session_state: st.session_state.cost_data        = None
if "run_error"        not in st.session_state: st.session_state.run_error        = None
if "log_files"        not in st.session_state: st.session_state.log_files        = []
if "pipeline_steps"   not in st.session_state: st.session_state.pipeline_steps   = []
if "pipeline_running" not in st.session_state: st.session_state.pipeline_running = False


# ── HELPERS ───────────────────────────────────────────────────────────────────
def _render_architect_detail(plan):
    with st.expander("Architect", expanded=False):
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
    with st.expander("Developer (Generator)", expanded=False):
        c1, c2, c3 = st.columns(3)
        c1.metric("Models",        len(project.models))
        c2.metric("Macros",        len(project.macros))
        c3.metric("Not Converted", len(project.not_converted))
        if project.not_converted:
            st.markdown("**Not Converted:**")
            for item in project.not_converted:
                st.markdown(f"- {item}")


def _render_fixer_detail(final_state):
    review_count = final_state.get("review_count", 0)
    if review_count:
        with st.expander("Fixer", expanded=False):
            st.info(f"Fixer ran {review_count} time(s). See Pipeline Logs → fixer_raw_* for details.")


def _fill_output_sections(placeholders: dict, final_state: dict, cost_data, log_files):
    """Fill all output section placeholders with real content."""

    with placeholders["agents_summary"]:
        if final_state.get("analysis"):          render_analyzer_detail(final_state["analysis"])
        if final_state.get("resolved_mappings"): render_resolver_detail(final_state["resolved_mappings"])
        if final_state.get("migration_plan"):    _render_architect_detail(final_state["migration_plan"])
        if final_state.get("dbt_project"):       _render_generator_detail(final_state["dbt_project"])
        if final_state.get("review"):            render_review_detail(final_state["review"])
        _render_fixer_detail(final_state)

    with placeholders["pipeline_logs"]:
        _EXCL = ("ingestion_blocks", "sttm_output", "sas_documentation")
        visible = [f for f in log_files if not any(f.stem.startswith(p) for p in _EXCL)]
        if visible:
            log_tabs = st.tabs([f.stem for f in visible])
            for tab, lf in zip(log_tabs, visible):
                with tab:
                    try:
                        content = json.loads(lf.read_text(encoding="utf-8"))
                        st.markdown(
                            f'<div class="log-box">{json.dumps(content, indent=2)}</div>',
                            unsafe_allow_html=True)
                    except Exception:
                        st.markdown(
                            f'<div class="log-box">{lf.read_text(encoding="utf-8")[:5000]}</div>',
                            unsafe_allow_html=True)
        else:
            st.info("No logs available.")

    with placeholders["output_summary"]:
        if final_state.get("dbt_project"):
            render_generated_files(final_state["dbt_project"])
        else:
            st.info("No dbt project generated.")

    with placeholders["cost_summary"]:
        if cost_data:
            render_cost_summary(cost_data)
        else:
            st.info("No cost data available.")

    with placeholders["documentation"]:
        render_documentation(final_state.get("sas_documentation"))

    with placeholders["sttm"]:
        render_sttm(final_state.get("sttm_data"))


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
    .zs-header { display:flex; align-items:center; gap:16px; margin-bottom:8px; }
    .zs-header img { height:40px; }
    .zs-header h1  { font-size:28px; font-weight:700; margin:0; }
    .zs-header p   { font-size:14px; color:#888; margin:0; }
    .log-box {
        background:#0f172a; color:#e2e8f0; padding:12px; border-radius:8px;
        font-family:monospace; font-size:12px; max-height:400px; overflow-y:auto;
        border:1px solid #334155; white-space:pre-wrap;
    }
    .human-review-alert {
        background:#fef3c7; border:2px solid #f59e0b;
        border-radius:8px; padding:16px; margin:12px 0;
    }
    .human-review-alert h3 { color:#92400e; margin:0 0 8px 0; }
    .human-review-alert p  { color:#78350f; margin:4px 0; font-size:14px; }
</style>
""", unsafe_allow_html=True)

logo_path = Path(__file__).parent / "assests" / "zs_logo.png"
ZS_LOGO_URL = ""
if logo_path.exists():
    ZS_LOGO_URL = f"data:image/png;base64,{base64.b64encode(logo_path.read_bytes()).decode()}"

st.markdown(f"""
<div class="zs-header">
    <img src="{ZS_LOGO_URL}" alt="ZS Logo">
    <div><h1>HPP Capabilities</h1>
    <p>SAS to dbt Agentic Code Migration</p></div>
</div>
""", unsafe_allow_html=True)


# ── LEFT SIDEBAR ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")
    st.subheader("API Key")
    api_key_input = st.text_input(
        "OpenAI API Key", type="password", placeholder="sk-...",
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
    **Pipeline:**
    1. Analyzer → 2. Resolver → 3. Architect
    4. Developer → 5. Reviewer ↔ Fixer
    6. Write Output → 7. Documenter → 8. STTM
    """)


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
                    except (UnicodeDecodeError, ValueError): continue
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
        up = st.file_uploader("Upload SAS Script", type=["sas","txt"], key="sas_upload")
        if up:
            for enc in ["utf-8","utf-8-sig","latin-1","cp1252"]:
                try:
                    sas_code = up.getvalue().decode(enc)
                    if sas_code.strip(): break
                except (UnicodeDecodeError, ValueError): continue
    with c2:
        up = st.file_uploader("Upload Column Mapping", type=["json","csv"], key="mapping_upload")
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
    run_clicked = st.button("🚀 Run Pipeline", disabled=not can_run,
                            type="primary", use_container_width=True)

# ── LAYOUT: shown once pipeline has been triggered at least once ──────────────
pipeline_ever_run = (
    st.session_state.final_state is not None
    or st.session_state.run_error is not None
    or len(st.session_state.pipeline_steps) > 0
    or run_clicked
)

if not pipeline_ever_run:
    st.stop()

st.divider()
col_main, col_progress = st.columns([3, 1])

# ── RIGHT: Pipeline Progress ──────────────────────────────────────────────────
with col_progress:
    st.markdown("### ⚙️ Pipeline Progress")
    timeline_placeholder = st.empty()
    # Render timeline from session state (shows steps from previous/current run)
    with timeline_placeholder:
        render_pipeline_timeline()

# ── CENTER: Output sections as expanders with placeholders ───────────────────
with col_main:
    st.markdown("### 📋 Pipeline Outputs")

    section_defs = [
        ("agents_summary",  "📊 Agents Summary"),
        ("pipeline_logs",   "📝 Pipeline Logs"),
        ("output_summary",  "⚙️ Output Summary"),
        ("cost_summary",    "💰 Cost Summary"),
        ("documentation",   "📄 Documentation"),
        ("sttm",            "🗺️ Source-to-Target Mapping"),
    ]

    placeholders = {}
    for key, label in section_defs:
        with st.expander(label, expanded=False):
            placeholders[key] = st.empty()
            # If results already exist (rerun from widget interaction), fill immediately
            if st.session_state.final_state is not None:
                pass  # filled below after expanders are created
            else:
                placeholders[key].info("⏳ Waiting for pipeline to complete...")

# ── EXECUTE PIPELINE (blocking — same proven pattern) ────────────────────────
if run_clicked and can_run:
    # Clear previous results
    st.session_state.run_error       = None
    st.session_state.final_state     = None
    st.session_state.cost_data       = None
    st.session_state.log_files       = []
    st.session_state.pipeline_steps  = []

    # Show spinners in all output sections while running
    for key in placeholders:
        placeholders[key].info("⏳ Pipeline running — please wait...")

    # Reset timeline to empty
    with timeline_placeholder:
        render_pipeline_timeline()

    # Parse inputs
    try:
        col_mappings = [ColumnMapping(**e) for e in json.loads(mapping_raw)]
    except Exception as e:
        st.session_state.run_error = f"Failed to parse mapping file: {e}"
        col_mappings = None

    if col_mappings is not None:
        conventions = DbtConventions()

        with st.spinner("Pipeline running..."):
            final_state, cost_data = run_pipeline(
                sas_code=sas_code,
                mappings=col_mappings,
                conventions=conventions,
                status_container=st.empty(),
                step_containers={},
            )

        # Update timeline with completed steps
        with timeline_placeholder:
            render_pipeline_timeline()

        if final_state is None:
            st.session_state.run_error = "Pipeline failed. Check logs for details."
        else:
            st.session_state.final_state = final_state
            st.session_state.cost_data   = cost_data
            st.session_state.log_files   = get_current_run_logs()

# ── FILL OUTPUT SECTIONS (from session state — works on both initial run and reruns)
if st.session_state.run_error:
    with col_main:
        st.error(st.session_state.run_error)

if st.session_state.final_state is not None:
    final_state = st.session_state.final_state
    cost_data   = st.session_state.cost_data
    log_files   = st.session_state.log_files

    _fill_output_sections(placeholders, final_state, cost_data, log_files)

    # Human review alerts
    with col_main:
        if final_state.get("resolved_mappings") and final_state["resolved_mappings"].unresolved_tables:
            rm = final_state["resolved_mappings"]
            st.markdown(f"""
            <div class="human-review-alert">
                <h3>👤 Human Review Required — Unresolved Mappings</h3>
                <p>{'<br>'.join('• ' + t for t in rm.unresolved_tables)}</p>
                <p>Update the column mapping file and re-run.</p>
            </div>""", unsafe_allow_html=True)

        if final_state.get("review") and not final_state["review"].is_valid:
            errors = [i for i in final_state["review"].issues if i.severity == "error"]
            if errors:
                st.markdown(f"""
                <div class="human-review-alert">
                    <h3>👤 Human Review Required — Logic Parity Issues</h3>
                    <p>{'<br>'.join(f"• <b>{i.file}</b>: {i.issue}" for i in errors[:10])}</p>
                </div>""", unsafe_allow_html=True)

    # Status footer
    final_status = final_state.get("status", "unknown")
    color = "#1e8c45" if final_status in ("done","complete","complete_with_warnings") else "#c0392b"
    label = {
        "done":                   "Pipeline completed successfully",
        "complete":               "Pipeline completed successfully",
        "complete_with_warnings": "Pipeline completed with warnings",
        "halted":                 f"Pipeline halted — {final_state.get('error','see logs')}",
    }.get(final_status, f"Pipeline ended — {final_status}")

    st.markdown(
        f"<div style='margin-top:16px; padding:10px 16px; background:{color}22; "
        f"border:1px solid {color}; border-radius:8px; text-align:center;'>"
        f"<span style='color:{color}; font-weight:600;'>● {label}</span></div>",
        unsafe_allow_html=True,
    )