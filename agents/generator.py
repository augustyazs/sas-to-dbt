from state.graph_state import GraphState
from models.schemas import DbtProject
from tools.llm_client import call_llm
from config.prompts import GENERATOR_SYSTEM, GENERATOR_USER
from utils.logger import log_step


def generator_node(state: GraphState) -> dict:
    """Generate dbt project files from analysis, resolved mappings, and migration plan."""
    print("\n[DEVELOPER] Generating dbt project...")

    analysis    = state["analysis"]
    resolved    = state["resolved_mappings"]
    conventions = state["conventions"]
    plan        = state.get("migration_plan")

    plan_json = plan.model_dump_json(indent=2) if plan else "{}"

    user_prompt = GENERATOR_USER.format(
        analysis_json=analysis.model_dump_json(indent=2),
        migration_plan_json=plan_json,
        resolved_mappings_json=resolved.model_dump_json(indent=2),
        conventions_json=conventions.model_dump_json(indent=2),
    )

    result = call_llm(GENERATOR_SYSTEM, user_prompt, step_name="generator")

    def _sanitize(raw_list, label):
        out = []
        for i, item in enumerate(raw_list or []):
            if isinstance(item, dict) and "path" in item and "content" in item:
                out.append(item)
            else:
                print(f"  WARNING: {label}[{i}] malformed — skipping.")
        return out

    result["models"] = _sanitize(result.get("models", []), "models")
    result["macros"] = _sanitize(result.get("macros", []), "macros")

    project = DbtProject(**result)

    print(f"  Models generated : {len(project.models)}")
    print(f"  Macros generated : {len(project.macros)}")
    print(f"  Not converted    : {len(project.not_converted)}")

    log_step("generator_output", project)   # renamed from developer_output
    return {"dbt_project": project, "status": "generated"}