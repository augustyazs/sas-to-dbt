import json
from state.graph_state import GraphState
from models.schemas import DbtProject
from tools.llm_client import call_llm
from config.prompts import GENERATOR_SYSTEM, GENERATOR_USER
from utils.logger import log_step


def _merge_models(models: list[dict], target_platform: str) -> str:
    """
    Merge multiple generated files into one self-contained script.
    Preserves all content with section headers so nothing is lost.
    """
    sections = []
    for m in models:
        path    = m.get("path", "unknown")
        content = m.get("content", "").strip()
        header  = f"# {'=' * 60}\n# {path}\n# {'=' * 60}"
        sections.append(f"{header}\n\n{content}")
    return "\n\n\n".join(sections)


def generator_node(state: GraphState) -> dict:
    """Generate target platform output from analysis, resolved mappings, and migration plan."""
    target_platform = state.get("target_platform", "dbt")
    source_language = state.get("detected_language", "SAS")

    print(f"\n[GENERATOR] Generating {target_platform} project from {source_language} analysis...")

    analysis        = state["analysis"]
    resolved        = state["resolved_mappings"]
    output_conv     = state.get("output_conventions", {})
    plan            = state.get("migration_plan")

    # For non-dbt targets the architect was bypassed — migration_plan is empty
    plan_json = plan.model_dump_json(indent=2) if plan else "{}"

    # Pass output_conventions as the conventions parameter
    # (replaces the old static dbt_conventions.json for non-dbt targets)
    conventions_json = json.dumps(output_conv, indent=2) if output_conv else "{}"

    user_prompt = GENERATOR_USER.format(
        target_platform        = target_platform,
        source_language        = source_language,
        output_conventions     = conventions_json,
        analysis_json          = analysis.model_dump_json(indent=2),
        migration_plan_json    = plan_json,
        resolved_mappings_json = resolved.model_dump_json(indent=2),
        conventions_json       = conventions_json,
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

    # ── Non-dbt: collapse to single file ─────────────────────────────────────
    # For PySpark/Scala/SQL the generator sometimes still produces multiple
    # staged files despite the prompt. Merge them into one file here.
    if target_platform != "dbt" and len(result["models"]) > 1:
        print(f"  Collapsing {len(result['models'])} files into single output file...")
        script_stem = state.get("source_filename", "output").rsplit(".", 1)[0]
        merged_content = _merge_models(result["models"], target_platform)
        result["models"] = [{
            "path":    f"src/{script_stem}.py" if target_platform == "pyspark" else f"src/{script_stem}.scala" if target_platform == "scala" else f"sql/{script_stem}.sql",
            "content": merged_content,
        }]

    # Ensure required DbtProject fields exist (may be empty for non-dbt targets)
    result.setdefault("dbt_project_yml", "")
    result.setdefault("sources_yml", "")
    result.setdefault("schema_yml", "")
    result.setdefault("not_converted", [])

    project = DbtProject(**result)

    print(f"  Models generated : {len(project.models)}")
    print(f"  Macros generated : {len(project.macros)}")
    print(f"  Not converted    : {len(project.not_converted)}")

    log_step("generator_output", project)
    return {"dbt_project": project, "status": "generated"}