from state.graph_state import GraphState
from config.settings import MAX_REVIEW_RETRIES


def after_scout(state: GraphState) -> str:
    if state.get("status") == "error":
        return "halt"
    return "analyzer"


def after_analyzer(state: GraphState) -> str:
    if state.get("status") == "error":
        return "halt"
    return "resolver"


def after_resolver(state: GraphState) -> str:
    if state.get("status") == "unresolved_critical":
        return "halt"
    # Architect only for dbt — all other targets go directly to generator
    if state.get("target_platform", "dbt") == "dbt":
        return "architect"
    return "generator"


def after_reviewer_fixer(state: GraphState) -> str:
    status       = state.get("status", "")
    review_count = state.get("review_count", 0)

    if status in ("complete", "complete_with_warnings"):
        return "write_output"

    if status == "needs_fix" and review_count < MAX_REVIEW_RETRIES:
        return "fixer"

    # Max retries exhausted — write what we have
    return "write_output"