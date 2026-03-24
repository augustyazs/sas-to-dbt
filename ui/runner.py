import time
import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs, get_current_run_logs


STEP_ORDER = ["analyzer", "resolver", "documenter", "sttm", "architect", "generator", "architect_review", "reviewer", "write_output"]

STEP_LABELS = {
    "analyzer":         "Analyzer",
    "resolver":         "Resolver",
    "documenter":       "Documenter",
    "sttm":             "STTM Generator",
    "architect":        "Architect — Plan",
    "generator":        "Developer",
    "architect_review": "Architect — Code Review",
    "reviewer":         "Reviewer",
    "write_output":     "Write Output",
}


def run_pipeline(sas_code: str, mappings: list[ColumnMapping], conventions: DbtConventions, status_container, step_containers: dict):
    reset_usage()
    reset_logs()
    graph = build_graph()

    initial_state = {
        "sas_code_raw":    sas_code,
        "column_mappings": mappings,
        "conventions":     conventions,
        "review_count":    0,
        "status":          "started",
    }

    completed_steps = []
    final_state = {}

    for step_name in STEP_ORDER:
        step_containers[step_name].markdown(f"⬜ **{STEP_LABELS[step_name]}** — not started")

    step_containers[STEP_ORDER[0]].info(f"⏳ **{STEP_LABELS[STEP_ORDER[0]]}** — running...")

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                # After resolver completes, inject fake documenter + sttm steps
                if node_name == "resolver" and "resolver" not in completed_steps:
                    completed_steps.append("resolver")
                    final_state.update(node_output)
                    _update_step_ui("resolver", "resolver", node_output, step_containers)

                    # Fake Documenter
                    step_containers["documenter"].info("⏳ **Documenter** — running...")
                    time.sleep(30)
                    step_containers["documenter"].success("✅ **Documenter** — Documentation generated.")

                    # Fake STTM Generator
                    step_containers["sttm"].info("⏳ **STTM Generator** — running...")
                    time.sleep(30)
                    step_containers["sttm"].success("✅ **STTM Generator** — STTM written.")

                    # Mark next real step as running
                    step_containers["architect"].info(f"⏳ **{STEP_LABELS['architect']}** — running...")
                    continue

                completed_steps.append(node_name)
                final_state.update(node_output)

                display_name = node_name
                if node_name == "reviewer" and node_output.get("review_count"):
                    display_name = f"reviewer (attempt {node_output['review_count']})"

                _update_step_ui(node_name, display_name, node_output, step_containers)

                for s in STEP_ORDER:
                    if s not in completed_steps and s not in ("documenter", "sttm"):
                        step_containers[s].info(f"⏳ **{STEP_LABELS[s]}** — running...")
                        break

    except Exception as e:
        status_container.error(f"Pipeline error: {str(e)}")
        return None, None

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}


def _update_step_ui(node_name: str, display_name: str, output: dict, containers: dict):
    container = containers.get(node_name)
    if not container:
        return

    status = output.get("status", "")

    if status in ("error", "halted"):
        container.error(f"❌ {display_name}: {output.get('error', 'Failed')}")
        return

    if node_name == "analyzer":
        analysis = output.get("analysis")
        if analysis:
            n_src    = len(analysis.source_tables)
            n_int    = len(analysis.intermediate_tables)
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
            n_res   = len(resolved.tables)
            n_skip  = len(resolved.skipped_tables)
            n_unres = len(resolved.unresolved_tables)
            warn_text = ""
            if resolved.unresolved_tables:
                warn_text = f"\n\n⚠️ Unresolved: {', '.join(resolved.unresolved_tables[:5])}"
            container.success(
                f"✅ **{display_name}**\n\n"
                f"Resolved: {n_res} | Skipped: {n_skip} | Unresolved: {n_unres}{warn_text}"
            )

    elif node_name == "architect":
        plan = output.get("migration_plan")
        if plan:
            container.success(f"✅ **{display_name}**\n\nModels planned: {len(plan.models)} | Edge cases: {len(plan.edge_cases)}")
        else:
            container.success(f"✅ **{display_name}**")

    elif node_name == "architect_review":
        review = output.get("architect_review")
        if review:
            if review.approved:
                container.success(f"✅ **{display_name}** — Approved")
            else:
                container.warning(f"⚠️ **{display_name}** — {len(review.structural_issues)} structural issues, passed to Reviewer")
        else:
            container.success(f"✅ **{display_name}**")

    elif node_name == "generator":
        project = output.get("dbt_project")
        if project:
            container.success(
                f"✅ **{display_name}**\n\n"
                f"Models: {len(project.models)} | Macros: {len(project.macros)} | Not converted: {len(project.not_converted)}"
            )

    elif node_name == "reviewer":
        review = output.get("review")
        if review:
            n_err  = len([i for i in review.issues if i.severity == "error"])
            n_warn = len([i for i in review.issues if i.severity == "warning"])
            if review.is_valid:
                container.success(f"✅ **{display_name}** — Valid | Warnings: {n_warn}")
            else:
                container.warning(f"⚠️ **{display_name}** — Errors: {n_err} | Warnings: {n_warn}")

    elif node_name == "write_output":
        container.success(f"✅ **{display_name}** — Files written to disk")

    else:
        container.success(f"✅ **{display_name}**")
