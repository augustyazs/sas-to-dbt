import json
from state.graph_state import GraphState
from models.schemas import ReviewResult, DbtProject, DbtFile
from tools.llm_client import call_llm
from config.prompts import REVIEWER_SYSTEM, REVIEWER_USER, FIX_SYSTEM, FIX_USER
from utils.logger import log_step


def reviewer_node(state: GraphState) -> dict:
    """Review generated dbt for logic parity with original SAS."""
    review_count = state.get("review_count", 0) + 1
    print(f"\n[REVIEWER] Review attempt {review_count}...")

    project        = state["dbt_project"]
    analysis       = state["analysis"]
    resolved       = state["resolved_mappings"]
    sas_code_clean = state.get("sas_code_clean", "")

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
        print(f"  Errors found — sending {len(errors)} error-flagged file(s) to fixer...")
        fixed_project = _fix_issues(project, review, resolved, sas_code_clean, review_count)
        return {
            "dbt_project":  fixed_project,
            "review":       review,
            "review_count": review_count,
            "status":       "needs_review",
        }

    status = "complete" if review.is_valid else "complete_with_warnings"
    return {"review": review, "review_count": review_count, "status": status}


def _extract_files_to_fix(project: DbtProject, review: ReviewResult) -> dict[str, str]:
    """Return only the files referenced in error-level issues."""
    error_files = {
        i.file.strip()
        for i in review.issues
        if i.severity == "error" and i.file
    }

    files: dict[str, str] = {}

    for model in project.models:
        name = model.path.split("/")[-1]
        stem = name[:-4] if name.endswith(".sql") else name
        if model.path in error_files or name in error_files or stem in error_files:
            files[model.path] = model.content

    for macro in project.macros:
        name = macro.path.split("/")[-1]
        stem = name[:-4] if name.endswith(".sql") else name
        if macro.path in error_files or name in error_files or stem in error_files:
            files[macro.path] = macro.content

    source_related = any(
        "source" in i.file.lower() or "sources" in i.issue.lower()
        for i in review.issues if i.severity == "error"
    )
    schema_related = any(
        "schema" in i.file.lower() or "schema" in i.issue.lower()
        for i in review.issues if i.severity == "error"
    )
    project_related = any(
        "dbt_project" in i.file.lower() or "dbt_project" in i.issue.lower()
        for i in review.issues if i.severity == "error"
    )

    if source_related and project.sources_yml:
        files["models/sources.yml"] = project.sources_yml
    if schema_related and project.schema_yml:
        files["models/schema.yml"] = project.schema_yml
    if project_related and project.dbt_project_yml:
        files["dbt_project.yml"] = project.dbt_project_yml

    # Fallback: if no files matched by name, send everything
    if not files:
        print("  WARNING: no files matched error issue names — sending full project to fixer")
        for model in project.models:
            files[model.path] = model.content
        for macro in project.macros:
            files[macro.path] = macro.content
        if project.sources_yml:
            files["models/sources.yml"] = project.sources_yml
        if project.schema_yml:
            files["models/schema.yml"] = project.schema_yml
        if project.dbt_project_yml:
            files["dbt_project.yml"] = project.dbt_project_yml

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
    for item in fix_result.get("not_converted", []) or []:
        if item not in merged_not_converted:
            merged_not_converted.append(item)

    return DbtProject(
        dbt_project_yml=_prefer_nonempty(fix_result.get("dbt_project_yml"), original.dbt_project_yml),
        sources_yml=_prefer_nonempty(fix_result.get("sources_yml"),     original.sources_yml),
        schema_yml=_prefer_nonempty(fix_result.get("schema_yml"),       original.schema_yml),
        models=merged_models,
        macros=merged_macros,
        not_converted=merged_not_converted,
    )


def _fix_issues(
    project: DbtProject,
    review: ReviewResult,
    resolved,
    sas_code_clean: str,
    review_count: int,
) -> DbtProject:
    """Fix review errors — send only error-flagged files, log raw fixer output, merge back."""
    files_to_fix = _extract_files_to_fix(project, review)

    user_prompt = FIX_USER.format(
        issues_json=review.model_dump_json(indent=2),
        files_to_fix_json=json.dumps(files_to_fix, indent=2),
        resolved_mappings_json=resolved.model_dump_json(indent=2),
        sas_code_clean=sas_code_clean,
    )

    result = call_llm(FIX_SYSTEM, user_prompt, step_name="fixer")

    # Log only the raw fixer response — what the LLM actually returned.
    # This is the useful debug artifact: it should contain only the broken files.
    # The merged project is not logged separately — it lives in state and will
    # appear in the next reviewer_attempt log if another pass runs.
    log_step(f"fixer_attempt_{review_count}", result, is_pydantic=False)

    return _merge_fix(project, result)