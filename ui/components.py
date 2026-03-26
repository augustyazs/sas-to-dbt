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


# ── Pipeline Progress ─────────────────────────────────────────────────────────

_SECTION_ORDER  = ["agents", "review", "documents"]
_SECTION_TITLES = {
    "agents":    "AGENTS",
    "review":    "REVIEWER / FIXER LOOP",
    "documents": "DOCUMENTS",
}

_DOT_STYLES = {
    "pending": ("border:2px solid #334155;background:#0f172a;", ""),
    "running": ("border:2px solid #3b82f6;background:#1d4ed8;", "box-shadow:0 0 8px #3b82f6,0 0 14px #1d4ed880;"),
    "done":    ("border:2px solid #16a34a;background:#15803d;", "box-shadow:0 0 8px #22c55e,0 0 14px #15803d80;"),
    "error":   ("border:2px solid #ca8a04;background:#854d0e;", "box-shadow:0 0 8px #facc15,0 0 14px #ca8a0480;"),
}
_LABEL_COLORS = {
    "pending": ("#475569", "400"),
    "running": ("#93c5fd", "700"),
    "done":    ("#86efac", "700"),
    "error":   ("#fde047", "700"),
}
_META_COLORS = {
    "pending": "#475569",
    "running": "#60a5fa",
    "done":    "#4ade80",
    "error":   "#fde047",
}


def _section_divider(title: str) -> str:
    return (
        '<div style="display:flex;align-items:center;gap:6px;margin:14px 0 10px 0;">'
        '<div style="width:24px;height:1px;background:#334155;flex-shrink:0;"></div>'
        f'<span style="font-size:9px;color:#64748b;font-weight:700;'
        f'letter-spacing:0.12em;white-space:nowrap;">{title}</span>'
        '<div style="width:24px;height:1px;background:#334155;flex-shrink:0;"></div>'
        '</div>'
    )

def _meta_text(step: dict) -> str:
    status  = step.get("status", "pending")
    elapsed = step.get("elapsed")
    color   = _META_COLORS.get(status, "#475569")
    if elapsed is not None:
        text = f"Completed &nbsp;·&nbsp; {_fmt_time(elapsed)}"
    elif status == "running":
        text = "In Progress…"
    else:
        text = "Not Started"
    return f'<div style="font-size:12px;color:{color};margin-top:2px;text-align:right;">{text}</div>'


def _render_section_rows(rows: list[dict]) -> str:
    """Render a section's timeline rows with one continuous vertical line, right-aligned."""
    if not rows:
        return (
            '<div style="padding:2px 26px 8px 0;color:#475569;'
            'font-size:12px;font-style:italic;text-align:right;">Waiting…</div>'
        )

    # Outer wrapper: padding-right leaves room for dot + line on the right
    html = (
        '<div style="position:relative;padding-right:26px;'
        'padding-top:4px;padding-bottom:4px;">'
    )
    # Continuous line on the RIGHT side
    html += (
        '<div style="position:absolute;right:6px;top:0;bottom:0;'
        'width:2px;background:#1e293b;z-index:0;"></div>'
    )

    for i, step in enumerate(rows):
        status    = step.get("status", "pending")
        label     = step.get("label", "")
        agent_num = step.get("agent_num")
        base, glow = _DOT_STYLES.get(status, _DOT_STYLES["pending"])
        lc, lw     = _LABEL_COLORS.get(status, ("#475569", "400"))

        mb = "margin-bottom:10px;" if i < len(rows) - 1 else "margin-bottom:4px;"
        dot_top = "14px" if agent_num else "4px"

        # Dot: right:-26px aligns its center (right:7px) with line center (right:7px)
        dot_html = (
            f'<div style="position:absolute;right:-26px;top:{dot_top};'
            f'width:14px;height:14px;border-radius:50%;{base}{glow}z-index:1;"></div>'
        )

        num_html = ""
        if agent_num is not None:
            num_html = (
                f'<div style="font-size:10px;color:#475569;'
                f'font-weight:500;line-height:1.2;text-align:right;">Agent {agent_num}</div>'
            )

        html += (
            f'<div style="position:relative;{mb}text-align:right;">'
            f'{dot_html}'
            f'{num_html}'
            f'<div style="font-size:15px;font-weight:{lw};color:{lc};line-height:1.3;">{label}</div>'
            f'{_meta_text(step)}'
            f'</div>'
        )

    html += '</div>'
    return html


def render_pipeline_progress(slot, steps: list[dict], write_output_status: str = "pending"):
    """Render the left-aligned timeline pipeline progress panel."""

    # Group steps preserving insertion order within each section
    grouped: dict[str, list[dict]] = {s: [] for s in _SECTION_ORDER}
    for step in steps:
        sec = step.get("section", "agents")
        if sec in grouped:
            grouped[sec].append(step)

    # Outer panel — right-aligned, with right padding as gap from separator
    html = '<div style="padding-right:14px;direction:rtl;">'
    html += '<div style="direction:ltr;">'  # re-ltr all inner content

    # Header — centered, no emoji, all caps, larger than agent name font
    html += (
        '<div style="font-size:18px;font-weight:800;color:#e2e8f0;'
        'letter-spacing:0.08em;text-align:center;margin-bottom:4px;">'
        'PIPELINE PROGRESS</div>'
    )

    for sec in _SECTION_ORDER:
        html += _section_divider(_SECTION_TITLES[sec])
        html += _render_section_rows(grouped[sec])

    html += '</div></div>'  # close ltr inner + rtl outer panel

    # Bottom output bar
    bar_cfg = {
        "pending": ("#1e293b",                              "#334155", "#475569", "○ &nbsp;Outputs pending"),
        "running": ("#1e3a5f",                              "#3b82f6", "#93c5fd", "◉ &nbsp;Generating outputs…"),
        "done":    ("linear-gradient(90deg,#052e16,#14532d)","#16a34a", "#4ade80", "✓ &nbsp;Outputs generated successfully"),
    }
    bg, border, color, text = bar_cfg.get(write_output_status, bar_cfg["pending"])
    html += (
        f'<div style="margin-top:14px;padding:8px 12px;border-radius:6px;'
        f'background:{bg};border:1px solid {border};color:{color};'
        f'font-size:13px;font-weight:600;text-align:center;letter-spacing:0.03em;">'
        f'{text}</div>'
    )

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
        df, use_container_width=True, hide_index=True,
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
            cols[3].write(f"${entry['cost_usd']:.4f}")