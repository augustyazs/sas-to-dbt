from state.graph_state import GraphState
from config.settings import MAX_REVIEW_RETRIES


def after_analyzer(state: GraphState) -> str:
    if state.get("status") == "error":
        return "halt"
    return "resolver"


def after_resolver(state: GraphState) -> str:
    if state.get("status") == "unresolved_critical":
        return "halt"
    return "architect"


def after_reviewer_fixer(state: GraphState) -> str:
    """Route after reviewer: send to fixer if errors exist, else exit loop."""
    status       = state.get("status", "")
    review_count = state.get("review_count", 0)

    if status in ("complete", "complete_with_warnings"):
        return "write_output"

    # needs_fix: errors found — send to fixer if retries remain
    if status == "needs_fix" and review_count < MAX_REVIEW_RETRIES:
        return "fixer"

    # Max retries exhausted — write whatever we have
    return "write_output"