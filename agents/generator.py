from state.graph_state import GraphState
from models.schemas import DbtProject
from tools.llm_client import call_llm
from config.prompts import GENERATOR_SYSTEM, GENERATOR_USER
from utils.logger import log_step


def generator_node(state: GraphState) -> dict:
    """Generate dbt project files from analysis and resolved mappings."""
    print("\n[GENERATOR] Generating dbt project...")

    analysis = state["analysis"]
    resolved = state["resolved_mappings"]
    conventions = state["conventions"]

    user_prompt = GENERATOR_USER.format(
        analysis_json=analysis.model_dump_json(indent=2),
        resolved_mappings_json=resolved.model_dump_json(indent=2),
        conventions_json=conventions.model_dump_json(indent=2),
    )

    result = call_llm(GENERATOR_SYSTEM, user_prompt, step_name="generator")
    project = DbtProject(**result)

    print(f"  Models generated: {len(project.models)}")
    print(f"  Macros generated: {len(project.macros)}")
    print(f"  Not converted: {len(project.not_converted)}")
    for nc in project.not_converted:
        print(f"    ⊘ {nc}")

    log_step("generator_output", project)

    return {"dbt_project": project, "status": "generated"}