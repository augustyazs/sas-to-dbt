from state.graph_state import GraphState
from models.schemas import DbtProject
from tools.llm_client import call_llm
from config.prompts import GENERATOR_SYSTEM, GENERATOR_USER
from utils.logger import log_step


def generator_node(state: GraphState) -> dict:
    """Generate dbt project files from analysis, resolved mappings, and migration plan."""
    print("\n[DEVELOPER] Generating dbt project...")

    analysis = state["analysis"]
    resolved = state["resolved_mappings"]
    conventions = state["conventions"]
    plan = state.get("migration_plan")

    plan_context = ""
    if plan:
        plan_context = f"\n\n## Migration Plan (follow this exactly)\n{plan.model_dump_json(indent=2)}"

    user_prompt = GENERATOR_USER.format(
        analysis_json=analysis.model_dump_json(indent=2),
        resolved_mappings_json=resolved.model_dump_json(indent=2),
        conventions_json=conventions.model_dump_json(indent=2),
    ) + plan_context

    result = call_llm(GENERATOR_SYSTEM, user_prompt, step_name="developer")
    project = DbtProject(**result)

    print(f"  Models generated: {len(project.models)}")
    print(f"  Macros generated: {len(project.macros)}")
    print(f"  Not converted: {len(project.not_converted)}")

    log_step("developer_output", project)

    return {"dbt_project": project, "status": "generated"}
