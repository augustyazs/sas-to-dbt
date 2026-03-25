from state.graph_state import GraphState
from models.schemas import ReviewResult
from tools.llm_client import call_llm
from config.prompts import REVIEWER_SYSTEM, REVIEWER_USER
from utils.logger import log_step


def reviewer_node(state: GraphState) -> dict:
    """Review generated dbt for logic parity and compile-readiness."""
    review_count = state.get("review_count", 0) + 1
    print(f"\n[REVIEWER] Review attempt {review_count}...")

    project = state["dbt_project"]
    analysis = state["analysis"]
    resolved = state["resolved_mappings"]

    user_prompt = REVIEWER_USER.format(
        generated_files_json=project.model_dump_json(indent=2),
        analysis_json=analysis.model_dump_json(indent=2),
        resolved_mappings_json=resolved.model_dump_json(indent=2),
    )

    result = call_llm(REVIEWER_SYSTEM, user_prompt, step_name=f"reviewer_attempt{review_count}")
    review = ReviewResult(**result)

    errors   = [i for i in review.issues if i.severity == "error"]
    warnings = [i for i in review.issues if i.severity == "warning"]

    print(f"  Valid   : {review.is_valid}")
    print(f"  Errors  : {len(errors)}, Warnings: {len(warnings)}")
    print(f"  Summary : {review.summary[:150]}")

    log_step(f"reviewer_attempt{review_count}", review)

    if not review.is_valid and errors:
        return {
            "review":       review,
            "review_count": review_count,
            "status":       "needs_fix",
        }

    status = "complete" if review.is_valid else "complete_with_warnings"
    return {"review": review, "review_count": review_count, "status": status}