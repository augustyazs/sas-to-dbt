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


# ── pipeline step containers (right sidebar) ──────────────────────────────────

def render_pipeline_steps():
    """Create timeline containers in the right sidebar.
    Returns (timeline_containers dict, loop_area container).
    """
    from ui.runner import STEP_ORDER, STEP_LABELS

    containers = {}

    st.markdown(
        "<p style='font-size:13px; color:#888; margin-bottom:8px;'>Agent steps</p>",
        unsafe_allow_html=True,
    )

    for step in STEP_ORDER:
        # Insert reviewer/fixer loop slot between generator and write_output
        if step == "write_output":
            st.markdown(
                "<p style='font-size:12px; color:#888; margin:6px 0 2px 8px;'>🔄 Reviewer / Fixer loop</p>",
                unsafe_allow_html=True,
            )
            loop_area = st.container()
            st.markdown("<div style='margin-top:4px;'></div>", unsafe_allow_html=True)

        containers[step] = st.empty()

    return containers, loop_area


# ── section expanders (center) — pre-rendered collapsed with spinners ─────────

def render_output_sections():
    """Render all output section expanders collapsed at start.
    Returns dict of placeholder containers keyed by section name.
    Each placeholder will be replaced with real content after pipeline completes.
    """
    sections = {}

    section_defs = [
        ("agents_summary",  "📊 Agents Summary"),
        ("pipeline_logs",   "📝 Pipeline Logs"),
        ("output_summary",  "⚙️ Output Summary"),
        ("cost_summary",    "💰 Cost Summary"),
        ("documentation",   "📄 Documentation"),
        ("sttm",            "🗺️ Source-to-Target Mapping"),
    ]

    for key, label in section_defs:
        with st.expander(label, expanded=False):
            sections[key] = st.empty()
            sections[key].info("⏳ Waiting for pipeline to complete...")

    return sections


# ── agents summary ────────────────────────────────────────────────────────────

def render_agents_summary(final_state: dict):
    """Detailed expandable panels for every agent, in pipeline order."""

    analysis  = final_state.get("analysis")
    resolved  = final_state.get("resolved_mappings")
    plan      = final_state.get("migration_plan")
    project   = final_state.get("dbt_project")
    review    = final_state.get("review")

    # Analyzer
    if analysis:
        with st.expander("Analyzer", expanded=False):
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
                    for n in analysis.complexity_notes:
                        st.markdown(f"- {n}")

    # Resolver
    if resolved:
        with st.expander("Resolver", expanded=False):
            tab1, tab2, tab3 = st.tabs(["Resolved", "Unresolved", "Warnings"])
            with tab1:
                for t in resolved.tables:
                    st.markdown(f"`{t.original_table}` → `{t.resolved_schema}.{t.resolved_table}` ({len(t.column_mappings)} cols)")
            with tab2:
                if resolved.unresolved_tables:
                    for t in resolved.unresolved_tables:
                        st.markdown(f"❌ `{t}`")
                else:
                    st.success("All tables resolved.")
            with tab3:
                for w in resolved.warnings:
                    st.markdown(f"⚠️ {w}")

    # Architect
    if plan:
        with st.expander("Architect", expanded=False):
            tab1, tab2 = st.tabs(["Planned Models", "Edge Cases"])
            with tab1:
                for m in plan.models:
                    st.markdown(f"**{m.name}** `{m.layer}` — {m.logic[:80]}")
            with tab2:
                if plan.edge_cases:
                    for ec in plan.edge_cases:
                        badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(ec.risk, "⚪")
                        st.markdown(f"{badge} **{ec.pattern}**\n\n{ec.recommendation}")
                else:
                    st.success("No edge cases flagged.")

    # Generator
    if project:
        with st.expander("Developer (Generator)", expanded=False):
            col1, col2, col3 = st.columns(3)
            col1.metric("Models",        len(project.models))
            col2.metric("Macros",        len(project.macros))
            col3.metric("Not Converted", len(project.not_converted))
            if project.not_converted:
                st.markdown("**Not Converted:**")
                for item in project.not_converted:
                    st.markdown(f"- {item}")

    # Reviewer
    if review:
        with st.expander("Reviewer", expanded=False):
            if review.is_valid:
                st.success(review.summary)
            else:
                st.warning(review.summary)
            errors   = [i for i in review.issues if i.severity == "error"]
            warnings = [i for i in review.issues if i.severity == "warning"]
            if errors:
                st.markdown("**Errors:**")
                for i in errors:
                    st.markdown(f"🔴 **{i.file}**: {i.issue}")
                    if i.fix_suggestion:
                        st.caption(f"Fix: {i.fix_suggestion}")
            if warnings:
                st.markdown("**Warnings:**")
                for i in warnings:
                    st.markdown(f"🟡 **{i.file}**: {i.issue}")

    # Fixer — show last review count's fixer info if available
    review_count = final_state.get("review_count", 0)
    if review_count and review and not review.is_valid:
        with st.expander("Fixer", expanded=False):
            st.info(f"Fixer ran {review_count} time(s). See Pipeline Logs → fixer_raw_* for per-pass details.")


# ── pipeline logs ─────────────────────────────────────────────────────────────

# Log files to exclude from the UI log viewer
_EXCLUDED_LOG_PREFIXES = (
    "ingestion_blocks",
    "sttm_output",
    "sas_documentation",
)


def render_pipeline_logs(log_files: list[Path]):
    if not log_files:
        st.info("No logs generated for this run.")
        return

    visible = [
        f for f in log_files
        if not any(f.stem.startswith(p) for p in _EXCLUDED_LOG_PREFIXES)
    ]

    if not visible:
        st.info("No displayable logs.")
        return

    log_tabs = st.tabs([f.stem for f in visible])
    for tab, log_file in zip(log_tabs, visible):
        with tab:
            try:
                content = json.loads(log_file.read_text(encoding="utf-8"))
                formatted = json.dumps(content, indent=2)
            except Exception:
                formatted = log_file.read_text(encoding="utf-8")[:5000]

            st.markdown(
                f'<div style="background:#0f172a; color:#e2e8f0; padding:12px; '
                f'border-radius:8px; font-family:monospace; font-size:12px; '
                f'max-height:400px; overflow-y:auto; border:1px solid #334155; '
                f'white-space:pre-wrap;">{formatted}</div>',
                unsafe_allow_html=True,
            )


# ── output summary (dbt files) ────────────────────────────────────────────────

def render_output_summary(project: DbtProject):
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

    cats: dict[str, list] = {"staging": [], "intermediate": [], "marts": [], "macros": [], "config": []}
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
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cost",    f"${cost_data['cost']['total_cost_usd']:.4f}")
    col2.metric("Input Tokens",  f"{cost_data['cost']['total_input_tokens']:,}")
    col3.metric("Output Tokens", f"{cost_data['cost']['total_output_tokens']:,}")
    col4.metric("LLM Calls",     cost_data['cost']['calls'])

    st.markdown("**Per-Step Breakdown**")

    # Header row
    hcols = st.columns([3, 2, 2, 2])
    hcols[0].markdown("**Agent**")
    hcols[1].markdown("**Input Tokens**")
    hcols[2].markdown("**Output Tokens**")
    hcols[3].markdown("**Cost**")
    st.markdown("<hr style='margin:4px 0;'>", unsafe_allow_html=True)

    for entry in cost_data["usage"]:
        cols = st.columns([3, 2, 2, 2])
        cols[0].write(entry["step"])
        cols[1].write(f"{entry['input_tokens']:,}")
        cols[2].write(f"{entry['output_tokens']:,}")
        cols[3].write(f"${entry['cost_usd']:.4f}")


# ── documentation ─────────────────────────────────────────────────────────────

def render_documentation(sas_documentation: str | None):
    if not sas_documentation:
        raw = _read_doc_file("sas_documentation.md")
        if raw:
            sas_documentation = raw.decode("utf-8")

    if not sas_documentation:
        st.info("No documentation generated for this run.")
        return

    st.markdown(sas_documentation)

    st.download_button(
        label="⬇️ Download Documentation (.md)",
        data=sas_documentation.encode("utf-8"),
        file_name="pipeline_documentation.md",
        mime="text/markdown",
    )


# ── sttm ─────────────────────────────────────────────────────────────────────

def render_sttm(sttm_data: dict | None):
    excel_bytes = _read_doc_file("sttm.xlsx")

    if not sttm_data:
        if excel_bytes:
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

    # Tabs — one per output table, no metrics row
    if len(tabs_data) == 1:
        _render_sttm_tab(tabs_data[0])
    else:
        tab_labels = [t.get("tab_name", f"Tab {i+1}") for i, t in enumerate(tabs_data)]
        st_tabs    = st.tabs(tab_labels)
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

    df = __import__("pandas").DataFrame(rows)
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