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


def _fmt_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}m {s:.0f}s"


# ── Pipeline Progress (timeline, right-aligned, dots on right) ────────────────

_SECTION_LABELS = {
    "agents":    "🤖 &nbsp; Agents",
    "review":    "🔄 &nbsp; Reviewer / Fixer",
    "documents": "📄 &nbsp; Documents",
}

_STATUS_META = {
    "pending": ("Not Started",  "pending"),
    "running": ("In Progress…", "running"),
    "done":    ("Completed",    "done"),
    "error":   ("Error",        "error"),
}


def render_pipeline_progress(slot, steps: list[dict], write_output_status: str = "pending"):
    """Render right-aligned timeline progress panel into an st.empty() slot.

    Sections always rendered in order: agents → review → documents.
    Reviewer/Fixer section header is always shown (even if no rows yet).
    Dots are on the RIGHT side of each row.
    Agent numbers shown as small label above agent name.
    Write Output bar shown only after sttm completes.
    """

    html = '<div class="progress-panel">'
    html += '<div class="progress-header">⚙️ &nbsp; Pipeline Progress</div>'

    # Group steps by section, preserving agent/review/documents order
    section_order = ["agents", "review", "documents"]
    grouped: dict[str, list[dict]] = {sec: [] for sec in section_order}
    for step in steps:
        sec = step.get("section", "agents")
        if sec in grouped:
            grouped[sec].append(step)

    all_rows: list[tuple[str, list[dict]]] = []
    for sec in section_order:
        all_rows.append((sec, grouped[sec]))

    # Flatten to compute total for last-row check (no line after last)
    flat: list[tuple[str, dict]] = []
    for sec, sec_steps in all_rows:
        for step in sec_steps:
            flat.append((sec, step))

    rendered = 0

    for sec, sec_steps in all_rows:
        sec_label = _SECTION_LABELS.get(sec, sec)
        html += f'<div class="progress-section-label">{sec_label}</div>'

        if not sec_steps:
            # Empty section — show a subtle placeholder row
            html += (
                '<div class="tl-row">'
                '<div class="tl-dot-col">'
                '<div class="tl-dot pending"></div>'
                '</div>'
                '<div class="tl-text">'
                '<div class="tl-label pending" style="font-style:italic;font-size:12px;">Waiting…</div>'
                '</div>'
                '</div>'
            )
            continue

        for step in sec_steps:
            status    = step.get("status", "pending")
            label     = step.get("label", step.get("key", ""))
            elapsed   = step.get("elapsed")
            agent_num = step.get("agent_num")
            meta_text, meta_cls = _STATUS_META.get(status, ("Not Started", "pending"))

            if elapsed is not None:
                meta_text = f"Completed &nbsp;·&nbsp; {_fmt_time(elapsed)}"
            elif status == "running":
                meta_text = "In Progress…"

            rendered += 1
            is_last = (rendered == len(flat))
            line_cls = "done" if status == "done" else ("running" if status == "running" else "")

            num_html = (
                f'<div class="tl-agent-num">Agent {agent_num}</div>'
                if agent_num is not None else ""
            )

            html += f"""
<div class="tl-row">
  <div class="tl-dot-col">
    <div class="tl-dot {status}"></div>
    {'<div class="tl-line ' + line_cls + '"></div>' if not is_last else ''}
  </div>
  <div class="tl-text">
    {num_html}
    <div class="tl-label {status}">{label}</div>
    <div class="tl-meta {meta_cls}">{meta_text}</div>
  </div>
</div>"""

    html += '</div>'  # close progress-panel

    # Write Output bar — only show as "done" once STTM completes
    wo_status = write_output_status
    wo_label = {
        "pending": "○ &nbsp; Outputs pending",
        "running": "◉ &nbsp; Generating outputs…",
        "done":    "✓ &nbsp; Outputs generated successfully",
    }.get(wo_status, "○ &nbsp; Outputs pending")
    html += f'<div class="wo-bar {wo_status}">{wo_label}</div>'

    slot.markdown(html, unsafe_allow_html=True)


# ── kept for import compatibility ─────────────────────────────────────────────
def render_pipeline_steps():
    return {}


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
        if "staging"        in path: cats["staging"].append((path, content))
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
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cost",    f"${cost_data['cost']['total_cost_usd']:.4f}")
    col2.metric("Input Tokens",  f"{cost_data['cost']['total_input_tokens']:,}")
    col3.metric("Output Tokens", f"{cost_data['cost']['total_output_tokens']:,}")
    col4.metric("LLM Calls",     cost_data['cost']['calls'])

    with st.expander("Per-Step Breakdown", expanded=False):
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
            cols[3].write(f"${entry['cost_usd']:.2f}")