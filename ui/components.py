import io
import json
import zipfile
import streamlit as st
from models.schemas import DbtProject, ReviewResult, SASAnalysis, ResolvedMappings


def render_file_uploaders():
    """Render SAS and mapping file uploaders. Returns (sas_code, mapping_raw)."""
    col1, col2 = st.columns(2)

    with col1:
        sas_file = st.file_uploader("Upload SAS Script", type=["sas", "txt"], key="sas_upload")
    with col2:
        mapping_file = st.file_uploader("Upload Column Mapping", type=["json", "csv"], key="mapping_upload")

    sas_code = None
    mapping_raw = None

    if sas_file:
        for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
            try:
                sas_code = sas_file.getvalue().decode(enc)
                if sas_code.strip():
                    break
            except (UnicodeDecodeError, ValueError):
                continue

    if mapping_file:
        mapping_raw = mapping_file.getvalue().decode("utf-8")

    return sas_code, mapping_raw


def render_sas_preview(sas_code: str):
    """Collapsible preview of uploaded SAS code."""
    with st.expander(f"SAS Script Preview ({len(sas_code):,} chars)", expanded=False):
        st.code(sas_code[:3000] + ("\n..." if len(sas_code) > 3000 else ""), language="sql")


def render_mapping_preview(mapping_raw: str):
    """Collapsible preview of mapping file."""
    try:
        data = json.loads(mapping_raw)
        with st.expander(f"Column Mapping Preview ({len(data)} entries)", expanded=False):
            st.json(data[:10])
            if len(data) > 10:
                st.caption(f"...and {len(data) - 10} more entries")
    except json.JSONDecodeError:
        st.warning("Could not parse mapping file as JSON.")


def render_pipeline_steps():
    """Create placeholder containers for each pipeline step. Returns dict of containers."""
    from ui.runner import STEP_ORDER, STEP_LABELS
    containers = {}
    for step in STEP_ORDER:
        containers[step] = st.empty()
        containers[step].info(f"⬜ {STEP_LABELS[step]} pending")
    return containers


def render_analyzer_detail(analysis: SASAnalysis):
    """Expandable detail view of analyzer output."""
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
    """Expandable detail view of resolver output."""
    with st.expander("Resolver Detail", expanded=False):
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


def render_review_detail(review: ReviewResult):
    """Expandable detail view of reviewer output."""
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


def render_generated_files(project: DbtProject):
    """Tabbed view of generated dbt files with download."""
    st.subheader("Generated dbt Files")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Models", len(project.models))
    col2.metric("Macros", len(project.macros))
    col3.metric("Not Converted", len(project.not_converted))
    col4.metric("Total Files", len(project.models) + len(project.macros) + 3)

    all_files = []

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
        nc_content = "# Blocks Not Converted to dbt\n\n" + "\n".join(f"- {item}" for item in project.not_converted)
        all_files.append(("NOT_CONVERTED.md", nc_content))

    categories = {"staging": [], "intermediate": [], "marts": [], "macros": [], "config": []}
    for path, content in all_files:
        if "staging" in path:
            categories["staging"].append((path, content))
        elif "intermediate" in path:
            categories["intermediate"].append((path, content))
        elif "marts" in path:
            categories["marts"].append((path, content))
        elif "macros" in path:
            categories["macros"].append((path, content))
        else:
            categories["config"].append((path, content))

    tabs = st.tabs(["Config/YAML", "Staging", "Intermediate", "Marts", "Macros", "Not Converted"])

    tab_map = [
        (tabs[0], categories["config"]),
        (tabs[1], categories["staging"]),
        (tabs[2], categories["intermediate"]),
        (tabs[3], categories["marts"]),
        (tabs[4], categories["macros"]),
    ]

    for tab, files in tab_map:
        with tab:
            if not files:
                st.info("No files in this category.")
                continue
            file_names = [f[0].split("/")[-1] for f in files]
            selected = st.selectbox("Select file", file_names, key=f"select_{id(tab)}")
            idx = file_names.index(selected)
            lang = "yaml" if files[idx][0].endswith(".yml") else "sql"
            st.code(files[idx][1], language=lang)

    with tabs[5]:
        if project.not_converted:
            for item in project.not_converted:
                st.markdown(f"- {item}")
        else:
            st.success("Everything was converted.")

    zip_buffer = _create_zip(all_files)
    st.download_button(
        label="⬇️ Download dbt Project (ZIP)",
        data=zip_buffer,
        file_name="dbt_project.zip",
        mime="application/zip",
    )


def render_cost_summary(cost_data: dict):
    """Render cost and token usage."""
    st.subheader("Cost Summary")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cost", f"${cost_data['cost']['total_cost_usd']:.4f}")
    col2.metric("Input Tokens", f"{cost_data['cost']['total_input_tokens']:,}")
    col3.metric("Output Tokens", f"{cost_data['cost']['total_output_tokens']:,}")
    col4.metric("LLM Calls", cost_data['cost']['calls'])

    with st.expander("Per-Step Breakdown", expanded=False):
        for entry in cost_data["usage"]:
            cols = st.columns([3, 2, 2, 2])
            cols[0].write(entry["step"])
            cols[1].write(f"In: {entry['input_tokens']:,}")
            cols[2].write(f"Out: {entry['output_tokens']:,}")
            cols[3].write(f"${entry['cost_usd']:.4f}")


def _create_zip(files: list[tuple[str, str]]) -> bytes:
    """Create in-memory ZIP from list of (path, content) tuples."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, content in files:
            zf.writestr(path, content)
    buf.seek(0)
    return buf.getvalue()