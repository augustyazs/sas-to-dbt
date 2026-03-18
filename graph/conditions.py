from state.graph_state import GraphState
from config.settings import MAX_REVIEW_RETRIES


def after_analyzer(state: GraphState) -> str:
    """Route after analyzer: proceed or halt on error."""
    if state.get("status") == "error":
        return "halt"
    return "resolver"


def after_resolver(state: GraphState) -> str:
    """Route after resolver: proceed, warn, or halt on critical unresolved."""
    if state.get("status") == "unresolved_critical":
        return "halt"
    return "generator"


def after_reviewer(state: GraphState) -> str:
    """Route after reviewer: complete, retry, or give up."""
    status = state.get("status", "")
    review_count = state.get("review_count", 0)

    if status in ("complete", "complete_with_warnings"):
        return "write_output"

    if status == "needs_review" and review_count < MAX_REVIEW_RETRIES:
        return "reviewer"

    return "write_output"


def after_architect_review(state: GraphState) -> str:
    """Route after architect review: proceed or loop back."""
    review = state.get("architect_review")
    if review and not review.approved:
        return "reviewer"
    return "reviewer"
