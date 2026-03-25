from state.graph_state import GraphState
from config.settings import MAX_REVIEW_RETRIES


def after_analyzer(state: GraphState) -> str:
    if state.get("status") == "error":
        return "halt"
    return "resolver"


def after_resolver(state: GraphState) -> str:
    if state.get("status") == "unresolved_critical":
        return "halt"
    return "documenter"


def after_reviewer(state: GraphState) -> str:
    status       = state.get("status", "")
    review_count = state.get("review_count", 0)

    if status in ("complete", "complete_with_warnings"):
        return "write_output"

    if status == "needs_review" and review_count < MAX_REVIEW_RETRIES:
        return "reviewer"

    return "write_output"