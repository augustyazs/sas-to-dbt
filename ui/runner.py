import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs


# Fixed linear steps — reviewer/fixer loop handled dynamically
STEP_ORDER = [
    "analyzer",
    "resolver",
    "architect",
    "generator",
    "write_output",
    "documenter",
    "sttm",
]

STEP_LABELS = {
    "analyzer":    "Analyzer",
    "resolver":    "Resolver",
    "architect":   "Architect — Plan",
    "generator":   "Developer",
    "write_output":"Write Output",
    "documenter":  "Documenter",
    "sttm":        "STTM Generator",
}


def run_pipeline(
    sas_code: str,
    mappings: list[ColumnMapping],
    conventions: DbtConventions,
    status_container,
    step_containers: dict,
):
    """Run the LangGraph pipeline with live Streamlit status updates.

    Uses the same blocking graph.stream() pattern as the previous working version.
    Reviewer/fixer loop steps are handled dynamically with st.empty() containers
    appended below the fixed steps.
    """
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

    completed_steps: list[str] = []
    final_state:     dict      = {}

    # Initialise all fixed steps to pending
    for step_name in STEP_ORDER:
        step_containers[step_name].markdown(
            f"⬜ **{STEP_LABELS[step_name]}** — not started"
        )

    step_containers[STEP_ORDER[0]].info(
        f"⏳ **{STEP_LABELS[STEP_ORDER[0]]}** — running..."
    )

    # Dynamic containers for reviewer/fixer loop — appended to the review_area
    review_area = step_containers.get("_review_area")
    review_attempt_containers: dict[int, dict] = {}

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                final_state.update(node_output)

                # ── Reviewer ─────────────────────────────────────────────────
                if node_name == "reviewer":
                    count  = node_output.get("review_count", 1)
                    review = node_output.get("review")
                    n_err  = len([i for i in review.issues if i.severity == "error"]) if review else 0
                    n_warn = len([i for i in review.issues if i.severity == "warning"]) if review else 0
                    passed = node_output.get("status", "") in ("complete", "complete_with_warnings")

                    if review_area:
                        with review_area:
                            reviewer_slot = st.empty()
                            fixer_slot    = st.empty()
                    else:
                        reviewer_slot = st.empty()
                        fixer_slot    = st.empty()

                    review_attempt_containers[count] = {
                        "reviewer": reviewer_slot,
                        "fixer":    fixer_slot,
                    }

                    if passed:
                        reviewer_slot.success(
                            f"✅ **Reviewer (pass {count})** — Valid | Warnings: {n_warn}"
                        )
                        fixer_slot.empty()
                        # Next: write_output
                        step_containers["write_output"].info(
                            f"⏳ **{STEP_LABELS['write_output']}** — running..."
                        )
                    else:
                        reviewer_slot.warning(
                            f"⚠️ **Reviewer (pass {count})** — Errors: {n_err} | Warnings: {n_warn} — sending to Fixer..."
                        )
                        fixer_slot.info(f"⏳ **Fixer (pass {count})** — running...")

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    count = final_state.get("review_count", 1)
                    slots = review_attempt_containers.get(count, {})
                    fixer_slot = slots.get("fixer")
                    if fixer_slot:
                        fixer_slot.info(
                            f"🔧 **Fixer (pass {count})** — Fixes applied, passing back to Reviewer..."
                        )

                # ── Fixed linear steps ────────────────────────────────────────
                else:
                    completed_steps.append(node_name)

                    if node_name in step_containers:
                        _update_step_ui(node_name, node_output, step_containers[node_name])

                    # Advance next fixed step to running
                    for s in STEP_ORDER:
                        if s not in completed_steps:
                            step_containers[s].info(
                                f"⏳ **{STEP_LABELS[s]}** — running..."
                            )
                            break

    except Exception as e:
        status_container.error(f"Pipeline error: {str(e)}")
        return None, None

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}


def _update_step_ui(node_name: str, output: dict, container):
    status = output.get("status", "")

    if status in ("error", "halted"):
        container.error(
            f"❌ **{STEP_LABELS.get(node_name, node_name)}**: {output.get('error', 'Failed')}"
        )
        return

    if node_name == "analyzer":
        analysis = output.get("analysis")
        if analysis:
            container.success(
                f"✅ **Analyzer**\n\n"
                f"Sources: {len(analysis.source_tables)} | "
                f"Intermediate: {len(analysis.intermediate_tables)} | "
                f"Transform blocks: {len(analysis.transformation_blocks)} | "
                f"Stripped: {len(output.get('ingestion_blocks', []))} ingestion blocks"
            )

    elif node_name == "resolver":
        resolved = output.get("resolved_mappings")
        if resolved:
            warn_text = ""
            if resolved.unresolved_tables:
                warn_text = f"\n\n⚠️ Unresolved: {', '.join(resolved.unresolved_tables[:5])}"
            container.success(
                f"✅ **Resolver**\n\n"
                f"Resolved: {len(resolved.tables)} | "
                f"Skipped: {len(resolved.skipped_tables)} | "
                f"Unresolved: {len(resolved.unresolved_tables)}{warn_text}"
            )

    elif node_name == "architect":
        plan = output.get("migration_plan")
        if plan:
            container.success(
                f"✅ **Architect — Plan**\n\n"
                f"Models planned: {len(plan.models)} | Edge cases: {len(plan.edge_cases)}"
            )
        else:
            container.success("✅ **Architect — Plan**")

    elif node_name == "generator":
        project = output.get("dbt_project")
        if project:
            container.success(
                f"✅ **Developer**\n\n"
                f"Models: {len(project.models)} | "
                f"Macros: {len(project.macros)} | "
                f"Not converted: {len(project.not_converted)}"
            )

    elif node_name == "write_output":
        container.success("✅ **Write Output** — dbt output generated successfully")

    elif node_name == "documenter":
        container.success("✅ **Documenter** — Documentation generated")

    elif node_name == "sttm":
        container.success("✅ **STTM Generator** — STTM written")

    else:
        container.success(f"✅ **{STEP_LABELS.get(node_name, node_name)}**")