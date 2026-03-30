import json
from state.graph_state import GraphState
from models.schemas import SASAnalysis
from tools.llm_client import call_llm
from tools.preprocessor import preprocess
from config.prompts import ANALYZER_SYSTEM, ANALYZER_USER
from utils.logger import log_step


def _coerce_nulls(result: dict) -> dict:
    """Coerce None values to empty strings/lists to prevent Pydantic validation errors."""
    for key in ("source_tables", "output_tables"):
        for t in result.get(key, []):
            t.setdefault("schema", t.pop("schema_name", "") or "")
            t.setdefault("schema_name", t.get("schema", ""))
            for f in ("description",):
                if t.get(f) is None:
                    t[f] = ""

    for t in result.get("intermediate_tables", []):
        for f in ("created_by", "logic_summary"):
            if t.get(f) is None:
                t[f] = ""

    for m in result.get("macros", []):
        for f in ("loop_description", "description"):
            if m.get(f) is None:
                m[f] = ""

    for mv in result.get("macro_variables", []):
        val = mv.get("value")
        if isinstance(val, list):
            mv["value"] = ", ".join(f"'{v}'" for v in val)
        elif val is None:
            mv["value"] = ""
        else:
            mv["value"] = str(val)

    for b in result.get("transformation_blocks", []):
        for f in ("type", "logic_summary", "sql_hint"):
            if b.get(f) is None:
                b[f] = ""
        ot = b.get("output_table")
        if isinstance(ot, list):
            b["output_table"] = ot[0] if ot else ""
        elif ot is None:
            b["output_table"] = ""

    for b in result.get("reporting_blocks", []):
        for f in ("description", "note"):
            if b.get(f) is None:
                b[f] = ""

    return result


def analyzer_node(state: GraphState) -> dict:
    """Preprocess source code, then extract structured metadata via LLM."""
    language           = state.get("detected_language", "SAS")
    input_conventions  = state.get("input_conventions", {})
    target_platform    = state.get("target_platform", "dbt")

    print(f"\n[ANALYZER] Preprocessing {language} script...")
    clean_code, ingestion_blocks = preprocess(
        raw_code           = state["sas_code_raw"],
        language           = language,
        input_conventions  = input_conventions,
    )

    print(f"  Stripped {len(ingestion_blocks)} ingestion/reporting blocks")
    if not clean_code.strip():
        return {
            "status":           "error",
            "error":            "No transformation logic found after stripping blocks.",
            "sas_code_clean":   "",
            "ingestion_blocks": ingestion_blocks,
        }

    # Serialize input_conventions for the prompt
    input_conv_str = json.dumps(input_conventions, indent=2) if input_conventions else "{}"

    print(f"[ANALYZER] Analyzing transformation logic via LLM...")
    user_prompt = ANALYZER_USER.format(
        source_language    = language,
        target_platform    = target_platform,
        input_conventions  = input_conv_str,
        source_code        = clean_code,
    )

    result = call_llm(ANALYZER_SYSTEM, user_prompt, step_name="analyzer")

    # Normalise schema field name (LLM sometimes returns "schema" vs "schema_name")
    for key in ("source_tables", "intermediate_tables", "output_tables"):
        for t in result.get(key, []):
            if "schema" in t and "schema_name" not in t:
                t["schema_name"] = t.pop("schema")

    result = _coerce_nulls(result)
    analysis = SASAnalysis(**result)

    print(f"  Source tables       : {[t.table for t in analysis.source_tables]}")
    print(f"  Intermediate tables : {[t.table for t in analysis.intermediate_tables]}")
    print(f"  Output tables       : {[t.table for t in analysis.output_tables]}")
    print(f"  Transformation blocks: {len(analysis.transformation_blocks)}")
    print(f"  Summary             : {analysis.logic_summary[:150]}...")

    log_step("analyzer_output", analysis)
    log_step("ingestion_blocks", ingestion_blocks, is_pydantic=False)

    return {
        "sas_code_clean":   clean_code,
        "ingestion_blocks": ingestion_blocks,
        "analysis":         analysis,
        "status":           "analyzed",
    }