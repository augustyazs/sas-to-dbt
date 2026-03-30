import json
from state.graph_state import GraphState
from models.schemas import DbtProject, DbtFile
from tools.llm_client import call_llm
from config.prompts import FIX_SYSTEM, FIX_USER
from utils.logger import log_step


def fixer_node(state: GraphState) -> dict:
    """Fix error-level issues flagged by the reviewer."""
    review          = state["review"]
    project         = state["dbt_project"]
    resolved        = state["resolved_mappings"]
    source_code     = state.get("sas_code_clean", "")
    review_count    = state.get("review_count", 0)
    target_platform = state.get("target_platform", "dbt")
    source_language = state.get("detected_language", "SAS")
    output_conv     = state.get("output_conventions", {})

    errors = [i for i in review.issues if i.severity == "error"]
    print(f"\n[FIXER] Fixing {len(errors)} error(s) — attempt {review_count} "
          f"({source_language} → {target_platform})...")

    files_to_fix = _extract_files_to_fix(project, review)

    user_prompt = FIX_USER.format(
        source_language        = source_language,
        target_platform        = target_platform,
        output_conventions     = json.dumps(output_conv, indent=2),
        issues_json            = review.model_dump_json(indent=2),
        files_to_fix_json      = json.dumps(files_to_fix, indent=2),
        resolved_mappings_json = resolved.model_dump_json(indent=2),
        source_code_clean      = source_code,
    )

    result = call_llm(FIX_SYSTEM, user_prompt, step_name="fixer")
    log_step(f"fixer_raw_{review_count}", result, is_pydantic=False)

    fixed_project = _merge_fix(project, result)

    print(f"  Fixed {len(result.get('models', []))} model(s), "
          f"{len(result.get('macros', []))} macro(s)")

    return {"dbt_project": fixed_project}


def _extract_files_to_fix(project: DbtProject, review) -> dict[str, str]:
    """Return only the files referenced in error-level issues."""
    error_files = {
        i.file.strip()
        for i in review.issues
        if i.severity == "error" and i.file
    }

    files: dict[str, str] = {}

    for model in project.models:
        name = model.path.split("/")[-1]
        stem = name.rsplit(".", 1)[0] if "." in name else name
        if model.path in error_files or name in error_files or stem in error_files:
            files[model.path] = model.content

    for macro in project.macros:
        name = macro.path.split("/")[-1]
        stem = name.rsplit(".", 1)[0] if "." in name else name
        if macro.path in error_files or name in error_files or stem in error_files:
            files[macro.path] = macro.content

    # Include config files if errors reference them
    source_related  = any("source" in i.file.lower() or "sources" in i.issue.lower()
                          for i in review.issues if i.severity == "error")
    schema_related  = any("schema" in i.file.lower() or "schema" in i.issue.lower()
                          for i in review.issues if i.severity == "error")
    project_related = any("dbt_project" in i.file.lower() or "dbt_project" in i.issue.lower()
                          for i in review.issues if i.severity == "error")

    if source_related  and project.sources_yml:   files["models/sources.yml"]  = project.sources_yml
    if schema_related  and project.schema_yml:    files["models/schema.yml"]   = project.schema_yml
    if project_related and project.dbt_project_yml: files["dbt_project.yml"]   = project.dbt_project_yml

    # Fallback: nothing matched by name — send full project
    if not files:
        print("  WARNING: no files matched error names — sending full project to fixer")
        for m in project.models:  files[m.path] = m.content
        for m in project.macros:  files[m.path] = m.content
        if project.sources_yml:   files["models/sources.yml"]  = project.sources_yml
        if project.schema_yml:    files["models/schema.yml"]   = project.schema_yml
        if project.dbt_project_yml: files["dbt_project.yml"]   = project.dbt_project_yml

    print(f"  Sending {len(files)} file(s) to fixer: {list(files.keys())}")
    return files


def _merge_fix(original: DbtProject, fix_result: dict) -> DbtProject:
    """Merge fixer partial output back into the full project."""
    fixed_models = {
        m["path"]: m["content"]
        for m in fix_result.get("models", [])
        if isinstance(m, dict) and "path" in m and "content" in m
    }
    fixed_macros = {
        m["path"]: m["content"]
        for m in fix_result.get("macros", [])
        if isinstance(m, dict) and "path" in m and "content" in m
    }

    merged_models = [
        DbtFile(path=m.path, content=fixed_models.get(m.path, m.content))
        for m in original.models
    ]
    for path, content in fixed_models.items():
        if path not in {m.path for m in original.models}:
            merged_models.append(DbtFile(path=path, content=content))

    merged_macros = [
        DbtFile(path=m.path, content=fixed_macros.get(m.path, m.content))
        for m in original.macros
    ]
    for path, content in fixed_macros.items():
        if path not in {m.path for m in original.macros}:
            merged_macros.append(DbtFile(path=path, content=content))

    def _prefer_nonempty(new_val, old_val):
        return new_val if new_val and str(new_val).strip() else old_val

    merged_not_converted = list(original.not_converted)
    for item in (fix_result.get("not_converted") or []):
        if item not in merged_not_converted:
            merged_not_converted.append(item)

    return DbtProject(
        dbt_project_yml = _prefer_nonempty(fix_result.get("dbt_project_yml"), original.dbt_project_yml),
        sources_yml     = _prefer_nonempty(fix_result.get("sources_yml"),      original.sources_yml),
        schema_yml      = _prefer_nonempty(fix_result.get("schema_yml"),       original.schema_yml),
        models          = merged_models,
        macros          = merged_macros,
        not_converted   = merged_not_converted,
    )