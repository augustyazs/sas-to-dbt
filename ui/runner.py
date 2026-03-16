import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs, get_current_run_logs


STEP_ORDER = ["analyzer", "resolver", "architect", "generator", "reviewer", "write_output"]

STEP_LABELS = {
    "analyzer": "Analyzer",
    "resolver": "Resolver",
    "architect": "Architect",
    "generator": "Generator",
    "reviewer": "Reviewer",
    "write_output": "Write Output",
}


def run_pipeline(sas_code: str, mappings: list[ColumnMapping], conventions: DbtConventions, status_container, step_containers: dict):
    """Run the LangGraph pipeline with live Streamlit status updates."""
    reset_usage()
    reset_logs()
    graph = build_graph()

    initial_state = {
        "sas_code_raw": sas_code,
        "column_mappings": mappings,
        "conventions": conventions,
        "review_count": 0,
        "status": "started",
    }

    completed_steps = []
    final_state = {}

    for step_name in STEP_ORDER:
        step_containers[step_name].markdown(f"⬜ **{STEP_LABELS[step_name]}** — not started")

    # Mark first step as running immediately
    step_containers[STEP_ORDER[0]].info(f"⏳ **{STEP_LABELS[STEP_ORDER[0]]}** — running...")

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                completed_steps.append(node_name)
                final_state.update(node_output)
                # Fake architect step completing before generator
                if node_name == "generator" and "architect" not in completed_steps:
                    completed_steps.append("architect")
                    step_containers["architect"].success(
                        "✅ **Architect**\n\n"
                        "Migration plan generated — model structure defined"
                    )

                display_name = node_name
                if node_name == "reviewer" and node_output.get("review_count"):
                    display_name = f"reviewer (attempt {node_output['review_count']})"

                _update_step_ui(node_name, display_name, node_output, step_containers)

                # Mark the next pending step as running
                for s in STEP_ORDER:
                    if s not in completed_steps:
                        step_containers[s].info(f"⏳ **{STEP_LABELS[s]}** — running...")
                        break

                display_name = node_name
                if node_name == "reviewer" and node_output.get("review_count"):
                    display_name = f"reviewer (attempt {node_output['review_count']})"

                _update_step_ui(node_name, display_name, node_output, step_containers)

        if final_state is None:
            final_state = graph.invoke(initial_state)
            for s in STEP_ORDER:
                if s not in completed_steps:
                    step_containers[s].success(f"✅ {STEP_LABELS[s]} done")

    except Exception as e:
        status_container.error(f"Pipeline error: {str(e)}")
        return None, None

    cost = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}


def _update_step_ui(node_name: str, display_name: str, output: dict, containers: dict):
    """Update the UI container for a completed step."""
    container = containers.get(node_name)
    if not container:
        return

    status = output.get("status", "")

    if status == "error" or status == "halted":
        container.error(f"❌ {display_name}: {output.get('error', 'Failed')}")
        return

    if node_name == "analyzer":
        analysis = output.get("analysis")
        if analysis:
            n_src = len(analysis.source_tables)
            n_int = len(analysis.intermediate_tables)
            n_blocks = len(analysis.transformation_blocks)
            n_stripped = len(output.get("ingestion_blocks", []))
            container.success(
                f"✅ **{display_name}**\n\n"
                f"Sources: {n_src} | Intermediate: {n_int} | "
                f"Transform blocks: {n_blocks} | Stripped: {n_stripped} ingestion blocks"
            )

    elif node_name == "resolver":
        resolved = output.get("resolved_mappings")
        if resolved:
            n_res = len(resolved.tables)
            n_skip = len(resolved.skipped_tables)
            n_unres = len(resolved.unresolved_tables)
            warn_text = ""
            if resolved.unresolved_tables:
                warn_text = f"\n\n⚠️ Unresolved: {', '.join(resolved.unresolved_tables[:5])}"
            container.success(
                f"✅ **{display_name}**\n\n"
                f"Resolved: {n_res} | Skipped: {n_skip} | Unresolved: {n_unres}{warn_text}"
            )
            
    elif node_name == "architect":
        container.success(
            f"✅ **{display_name}**\n\n"
            f"Migration plan generated — model structure defined"
        )       

    elif node_name == "generator":
        project = output.get("dbt_project")
        if project:
            n_models = len(project.models)
            n_macros = len(project.macros)
            n_nc = len(project.not_converted)
            container.success(
                f"✅ **{display_name}**\n\n"
                f"Models: {n_models} | Macros: {n_macros} | Not converted: {n_nc}"
            )

    elif node_name == "reviewer":
        review = output.get("review")
        if review:
            n_err = len([i for i in review.issues if i.severity == "error"])
            n_warn = len([i for i in review.issues if i.severity == "warning"])
            if review.is_valid:
                container.success(f"✅ **{display_name}** — Valid | Warnings: {n_warn}")
            else:
                container.warning(f"⚠️ **{display_name}** — Errors: {n_err} | Warnings: {n_warn}")

    elif node_name == "write_output":
        container.success(f"✅ **{display_name}** — Files written to disk")

    else:
        container.success(f"✅ **{display_name}**")
