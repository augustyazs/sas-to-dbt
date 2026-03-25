import io
import json
import zipfile
from pathlib import Path
import streamlit as st
from models.schemas import DbtProject, ReviewResult, SASAnalysis, ResolvedMappings
from config.settings import DOC_OUTPUT_DIR


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_doc_file(filename: str) -> bytes | None:
    p = DOC_OUTPUT_DIR / filename
    return p.read_bytes() if p.exists() else None


def _create_zip(files: list[tuple[str, str]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, content in files:
            zf.writestr(path, content)
    buf.seek(0)
    return buf.getvalue()


# ── input previews ────────────────────────────────────────────────────────────

def render_sas_preview(sas_code: str):
    with st.expander(f"SAS Script Preview ({len(sas_code):,} chars)", expanded=False):
        st.code(sas_code[:3000] + ("\n..." if len(sas_code) > 3000 else ""), language="sql")


def render_mapping_preview(mapping_raw: str):
    try:
        data = json.loads(mapping_raw)
        with st.expander(f"Column Mapping Preview ({len(data)} entries)", expanded=False):
            st.json(data[:10])
            if len(data) > 10:
                st.caption(f"...and {len(data) - 10} more entries")
    except json.JSONDecodeError:
        st.warning("Could not parse mapping file as JSON.")


# ── pipeline step containers (legacy — kept for API compat) ──────────────────

def render_pipeline_steps():
    """Kept for API compat. Returns empty dict — timeline is now rendered
    via render_pipeline_timeline() from session state."""
    return {}


# ── pipeline timeline (reads from session_state, survives reruns) ─────────────

def _fmt_time(seconds: float) -> str:
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}:{s:02d}"


_STATUS_STYLE = {
    "not_started": ("⚪", "#888888", "Not Started"),
    "in_progress": ("🔵", "#1d6ae5", "In Progress"),
    "completed":   ("🟢", "#1e8c45", "Completed"),
    "warning":     ("🟡", "#e5a91d", "Completed with warnings"),
    "failed":      ("🔴", "#c0392b", "Failed"),
}


def render_pipeline_timeline():
    """Render the pipeline progress timeline from session_state.pipeline_steps.
    Call this on every render — it reads stored state so it survives reruns.
    """
    steps = st.session_state.get("pipeline_steps", [])

    if not steps:
        st.caption("Pipeline not yet started.")
        return

    # Group into sections
    sections = {
        "agents":       ("🤖 Agents", []),
        "review":       ("🔄 Reviewer / Fixer", []),
        "documents":    ("📄 Documents", []),
        "write_output": ("", []),   # rendered separately at bottom
    }
    for s in steps:
        sec = s.get("section", "agents")
        if sec in sections:
            sections[sec][1].append(s)

    def _row(step):
        icon, color, status_text = _STATUS_STYLE.get(step["status"], _STATUS_STYLE["not_started"])
        time_str = f" ({_fmt_time(step['elapsed'])})" if step.get("elapsed") is not None else ""
        label    = step["label"]
        st.markdown(
            f"<div style='padding:3px 0 3px 10px; border-left:2px solid {color}; margin:2px 0;'>"
            f"<span style='font-weight:500; font-size:13px;'>{icon} {label}</span>"
            f"<br><span style='color:#888; font-size:11px;'>{status_text}{time_str}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    for sec_key in ("agents", "review", "documents"):
        header, rows = sections[sec_key]
        if not rows:
            continue
        st.markdown(
            f"<p style='font-size:11px; font-weight:600; color:#94a3b8; "
            f"margin:10px 0 4px 0; text-transform:uppercase; letter-spacing:.05em;'>"
            f"{header}</p>",
            unsafe_allow_html=True,
        )
        for s in rows:
            _row(s)

    # Write Output — distinct visual at bottom
    wo_rows = sections["write_output"][1]
    if wo_rows:
        wo = wo_rows[-1]
        icon, color, status_text = _STATUS_STYLE.get(wo["status"], _STATUS_STYLE["not_started"])
        time_str = f" ({_fmt_time(wo['elapsed'])})" if wo.get("elapsed") is not None else ""
        st.markdown(
            f"<div style='margin-top:12px; padding:8px 10px; "
            f"background:{'#0f2a1a' if wo['status']=='completed' else '#0f172a'}; "
            f"border-radius:6px; border:1px solid {color};'>"
            f"<span style='color:{color}; font-weight:600; font-size:13px;'>"
            f"{icon} All outputs generated{time_str}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ── step detail panels ────────────────────────────────────────────────────────

def render_analyzer_detail(analysis: SASAnalysis):
    with st.expander("Analyzer Detail", expanded=False):
        tab1, tab2, tab3, tab4 = st.tabs(["Source Tables", "Intermediates", "Transforms", "Summary"])
        with tab1:
            for t in analysis.source_tables:
                st.markdown(f"**{t.table}** ({t.access_method}) — {len(t.columns_used)} cols")
        with tab2:
            for t in analysis.intermediate_tables:
                st.markdown(f"**{t.table}** — {t.logic_summary[:100]}")
        with tab3:
            for b in analysis.transformation_blocks:
                st.markdown(f"**{b.name}** [{b.type}] → `{b.output_table}`")
        with tab4:
            st.write(analysis.logic_summary)
            if analysis.complexity_notes:
                st.markdown("**Complexity Notes:**")
                for note in analysis.complexity_notes:
                    st.markdown(f"- {note}")


def render_resolver_detail(resolved: ResolvedMappings):
    with st.expander("Resolver Detail", expanded=False):
        tab1, tab2, tab3 = st.tabs(["Resolved", "Unresolved", "Warnings"])
        with tab1:
            for t in resolved.tables:
                st.markdown(
                    f"`{t.original_table}` → `{t.resolved_schema}.{t.resolved_table}` "
                    f"({len(t.column_mappings)} cols)"
                )
        with tab2:
            if resolved.unresolved_tables:
                for t in resolved.unresolved_tables:
                    st.markdown(f"❌ `{t}`")
            else:
                st.success("All tables resolved.")
        with tab3:
            for w in resolved.warnings:
                st.markdown(f"⚠️ {w}")


def render_review_detail(review: ReviewResult):
    with st.expander("Reviewer Detail", expanded=False):
        if review.is_valid:
            st.success(review.summary)
        else:
            st.warning(review.summary)
        for issue in review.issues:
            icon = "🔴" if issue.severity == "error" else "🟡"
            st.markdown(f"{icon} **{issue.file}**: {issue.issue}")
            if issue.fix_suggestion:
                st.caption(f"Fix: {issue.fix_suggestion}")


# ── documentation ─────────────────────────────────────────────────────────────

def render_documentation(sas_documentation: str | None):
    st.subheader("📄 Pipeline Documentation")

    if not sas_documentation:
        raw = _read_doc_file("sas_documentation.md")
        if raw:
            sas_documentation = raw.decode("utf-8")

    if not sas_documentation:
        st.info("No documentation generated for this run.")
        return

    with st.expander("View Documentation", expanded=True):
        st.markdown(sas_documentation)

    st.download_button(
        label="⬇️ Download Documentation (.md)",
        data=sas_documentation.encode("utf-8"),
        file_name="pipeline_documentation.md",
        mime="text/markdown",
    )


# ── sttm ──────────────────────────────────────────────────────────────────────

def render_sttm(sttm_data: dict | None):
    st.subheader("🗺️ Source-to-Target Mapping (STTM)")

    excel_bytes = _read_doc_file("sttm.xlsx")

    if not sttm_data:
        if excel_bytes:
            st.info("STTM data not in session — showing download only.")
            st.download_button(
                label="⬇️ Download STTM (.xlsx)",
                data=excel_bytes,
                file_name="sttm.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("No STTM generated for this run.")
        return

    tabs_data = sttm_data.get("tabs", [])
    if not tabs_data:
        st.info("STTM generated but contains no tabs.")
        return

    if len(tabs_data) == 1:
        _render_sttm_tab(tabs_data[0])
    else:
        tab_labels = [t.get("tab_name", f"Tab {i+1}") for i, t in enumerate(tabs_data)]
        st_tabs = st.tabs(tab_labels)
        for st_tab, sttm_tab in zip(st_tabs, tabs_data):
            with st_tab:
                _render_sttm_tab(sttm_tab)

    if excel_bytes:
        st.download_button(
            label="⬇️ Download STTM (.xlsx)",
            data=excel_bytes,
            file_name="sttm.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def _render_sttm_tab(tab: dict):
    import pandas as pd

    desc = tab.get("description", "")
    if desc:
        st.caption(desc)

    rows = tab.get("rows", [])
    if not rows:
        st.info("No rows in this tab.")
        return

    COLUMNS = [
        "target_schema", "target_table", "target_column", "target_data_type",
        "transformation_rule",
        "source_schema", "source_table", "source_column", "source_data_type",
        "additional_comments",
    ]
    DISPLAY = [
        "Target Schema", "Target Table", "Target Column", "Target Type",
        "Transformation Rule",
        "Source Schema", "Source Table", "Source Column", "Source Type",
        "Comments",
    ]

    df = pd.DataFrame(rows)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[COLUMNS]
    df.columns = DISPLAY

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Transformation Rule": st.column_config.TextColumn(width="large"),
            "Comments":            st.column_config.TextColumn(width="medium"),
            "Target Column":       st.column_config.TextColumn(width="medium"),
            "Source Column":       st.column_config.TextColumn(width="medium"),
        },
    )


# ── generated dbt files ───────────────────────────────────────────────────────

def render_generated_files(project: DbtProject):
    st.subheader("⚙️ Output Summary")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Models",        len(project.models))
    col2.metric("Macros",        len(project.macros))
    col3.metric("Not Converted", len(project.not_converted))
    col4.metric("Total Files",   len(project.models) + len(project.macros) + 3)

    all_files: list[tuple[str, str]] = []
    if project.dbt_project_yml:
        all_files.append(("dbt_project.yml", project.dbt_project_yml))
    if project.sources_yml:
        all_files.append(("models/sources.yml", project.sources_yml))
    if project.schema_yml:
        all_files.append(("models/schema.yml", project.schema_yml))
    for m in project.models:
        all_files.append((m.path, m.content))
    for m in project.macros:
        all_files.append((m.path, m.content))
    if project.not_converted:
        nc = "# Blocks Not Converted to dbt\n\n" + "\n".join(f"- {i}" for i in project.not_converted)
        all_files.append(("NOT_CONVERTED.md", nc))

    cats: dict[str, list] = {
        "staging": [], "intermediate": [], "marts": [], "macros": [], "config": []
    }
    for path, content in all_files:
        if "staging"      in path: cats["staging"].append((path, content))
        elif "intermediate" in path: cats["intermediate"].append((path, content))
        elif "marts"        in path: cats["marts"].append((path, content))
        elif "macros"       in path: cats["macros"].append((path, content))
        else:                        cats["config"].append((path, content))

    tabs = st.tabs(["Config/YAML", "Staging", "Intermediate", "Marts", "Macros", "Not Converted"])
    tab_map = [
        (tabs[0], cats["config"]),
        (tabs[1], cats["staging"]),
        (tabs[2], cats["intermediate"]),
        (tabs[3], cats["marts"]),
        (tabs[4], cats["macros"]),
    ]
    for tab, files in tab_map:
        with tab:
            if not files:
                st.info("No files in this category.")
                continue
            names    = [f[0].split("/")[-1] for f in files]
            selected = st.selectbox("Select file", names, key=f"sel_{id(tab)}")
            idx      = names.index(selected)
            lang     = "yaml" if files[idx][0].endswith(".yml") else "sql"
            st.code(files[idx][1], language=lang)

    with tabs[5]:
        if project.not_converted:
            for item in project.not_converted:
                st.markdown(f"- {item}")
        else:
            st.success("Everything was converted.")

    st.download_button(
        label="⬇️ Download dbt Project (.zip)",
        data=_create_zip(all_files),
        file_name="dbt_project.zip",
        mime="application/zip",
    )


# ── cost summary ──────────────────────────────────────────────────────────────

def render_cost_summary(cost_data: dict):
    st.subheader("💰 Cost Summary")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cost",    f"${cost_data['cost']['total_cost_usd']:.4f}")
    col2.metric("Input Tokens",  f"{cost_data['cost']['total_input_tokens']:,}")
    col3.metric("Output Tokens", f"{cost_data['cost']['total_output_tokens']:,}")
    col4.metric("LLM Calls",     cost_data['cost']['calls'])

    with st.expander("Per-Step Breakdown", expanded=False):
        # Header row
        h = st.columns([3, 2, 2, 2])
        h[0].markdown("**Agent**")
        h[1].markdown("**Input Tokens**")
        h[2].markdown("**Output Tokens**")
        h[3].markdown("**Cost**")
        st.markdown("<hr style='margin:2px 0 6px 0;'>", unsafe_allow_html=True)

        for entry in cost_data["usage"]:
            cols = st.columns([3, 2, 2, 2])
            cols[0].write(entry["step"])
            cols[1].write(f"{entry['input_tokens']:,}")
            cols[2].write(f"{entry['output_tokens']:,}")
            cols[3].write(f"${entry['cost_usd']:.4f}")