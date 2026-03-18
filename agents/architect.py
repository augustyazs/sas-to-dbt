from state.graph_state import GraphState
from models.schemas import MigrationPlan, ArchitectReview, DbtProject
from tools.llm_client import call_llm
from config.prompts import (
    ARCHITECT_SYSTEM, ARCHITECT_USER,
    ARCHITECT_REVIEW_SYSTEM, ARCHITECT_REVIEW_USER,
)
from utils.logger import log_step


def architect_plan_node(state: GraphState) -> dict:
    """Plan the dbt model structure based on SAS analysis and resolved mappings."""
    print("\n[ARCHITECT] Planning migration structure...")

    analysis = state["analysis"]
    resolved = state["resolved_mappings"]
    conventions = state["conventions"]

    user_prompt = ARCHITECT_USER.format(
        analysis_json=analysis.model_dump_json(indent=2),
        resolved_mappings_json=resolved.model_dump_json(indent=2),
        conventions_json=conventions.model_dump_json(indent=2),
    )

    result = call_llm(ARCHITECT_SYSTEM, user_prompt, step_name="architect_plan")
    plan = MigrationPlan(**result)

    print(f"  Models planned: {len(plan.models)}")
    print(f"  Edge cases flagged: {len(plan.edge_cases)}")
    for ec in plan.edge_cases:
        print(f"    ⚠ [{ec.risk}] {ec.pattern}")

    log_step("architect_plan", plan)

    return {"migration_plan": plan, "status": "planned"}


def architect_review_node(state: GraphState) -> dict:
    """Review generated dbt code against the migration plan."""
    print("\n[ARCHITECT REVIEW] Reviewing generated code against plan...")

    plan = state["migration_plan"]
    project = state["dbt_project"]

    user_prompt = ARCHITECT_REVIEW_USER.format(
        plan_json=plan.model_dump_json(indent=2),
        generated_files_json=project.model_dump_json(indent=2),
    )

    result = call_llm(ARCHITECT_REVIEW_SYSTEM, user_prompt, step_name="architect_review")
    review = ArchitectReview(**result)

    print(f"  Approved: {review.approved}")
    print(f"  Structural issues: {len(review.structural_issues)}")
    print(f"  Summary: {review.summary[:150]}")

    log_step("architect_review", review)

    if not review.approved and review.structural_issues:
        print("  Sending structural fixes to Developer...")
        fixed_project = _apply_structural_fixes(project, review, state)
        return {
            "dbt_project": fixed_project,
            "architect_review": review,
            "status": "architect_reviewed",
        }

    return {"architect_review": review, "status": "architect_approved"}


def _apply_structural_fixes(project: DbtProject, review: ArchitectReview, state: GraphState) -> DbtProject:
    """Re-generate dbt project with structural fix instructions."""
    from config.prompts import FIX_SYSTEM, FIX_USER

    issues_text = "\n".join(
        f"- {issue['model']}: {issue['issue']} → Fix: {issue['fix']}"
        for issue in review.structural_issues
    )

    from models.schemas import ReviewResult, ReviewIssue
    fake_review = ReviewResult(
        is_valid=False,
        issues=[ReviewIssue(file=i.get("model", ""), issue=i.get("issue", ""), severity="error", fix_suggestion=i.get("fix", "")) for i in review.structural_issues],
        summary="Structural issues from Architect review",
    )

    user_prompt = FIX_USER.format(
        issues_json=fake_review.model_dump_json(indent=2),
        generated_files_json=project.model_dump_json(indent=2),
        resolved_mappings_json=state["resolved_mappings"].model_dump_json(indent=2),
    )

    result = call_llm(FIX_SYSTEM, user_prompt, step_name="architect_fix")
    return DbtProject(**result)
