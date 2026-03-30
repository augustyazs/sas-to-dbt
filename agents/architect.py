import json
from state.graph_state import GraphState
from models.schemas import MigrationPlan
from tools.llm_client import call_llm
from config.prompts import ARCHITECT_SYSTEM, ARCHITECT_USER
from utils.logger import log_step


def architect_plan_node(state: GraphState) -> dict:
    """Plan the model structure — dbt path only."""
    print("\n[ARCHITECT] Planning migration structure...")

    analysis         = state["analysis"]
    resolved         = state["resolved_mappings"]
    output_conv      = state.get("output_conventions", {})
    source_language  = state.get("detected_language", "SAS")
    target_platform  = state.get("target_platform", "dbt")

    user_prompt = ARCHITECT_USER.format(
        source_language       = source_language,
        target_platform       = target_platform,
        analysis_json         = analysis.model_dump_json(indent=2),
        resolved_mappings_json= resolved.model_dump_json(indent=2),
        output_conventions    = json.dumps(output_conv, indent=2),
    )

    result = call_llm(ARCHITECT_SYSTEM, user_prompt, step_name="architect_plan")
    plan   = MigrationPlan(**result)

    print(f"  Models planned : {len(plan.models)}")
    print(f"  Edge cases     : {len(plan.edge_cases)}")
    for ec in plan.edge_cases:
        print(f"    [{ec.risk}] {ec.pattern}")

    log_step("architect_plan", plan)
    return {"migration_plan": plan, "status": "planned"}