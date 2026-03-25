from state.graph_state import GraphState
from models.schemas import SASAnalysis
from tools.llm_client import call_llm
from tools.sas_preprocessor import preprocess_sas
from config.prompts import ANALYZER_SYSTEM, ANALYZER_USER
from utils.logger import log_step


def _coerce_nulls(result: dict) -> dict:
    """
    Coerce None values to empty strings for all string fields the LLM
    occasionally returns as null. Prevents Pydantic validation errors.
    """
    # source_tables / output_tables: schema_name
    for key in ("source_tables", "output_tables"):
        for t in result.get(key, []):
            if t.get("schema") is None:
                t["schema"] = ""
            if t.get("schema_name") is None:
                t["schema_name"] = ""
            if t.get("description") is None:
                t["description"] = ""

    # intermediate_tables
    for t in result.get("intermediate_tables", []):
        for field in ("created_by", "logic_summary"):
            if t.get(field) is None:
                t[field] = ""

    # macros
    for m in result.get("macros", []):
        for field in ("loop_description", "description"):
            if m.get(field) is None:
                m[field] = ""

    # macro_variables: value must be string
    for mv in result.get("macro_variables", []):
        val = mv.get("value")
        if isinstance(val, list):
            mv["value"] = ", ".join(f"'{str(v)}'" for v in val)
        elif val is None:
            mv["value"] = ""
        else:
            mv["value"] = str(val)

    # transformation_blocks
    for b in result.get("transformation_blocks", []):
        for field in ("type", "logic_summary", "sql_hint"):
            if b.get(field) is None:
                b[field] = ""
        # output_table must be a string — LLM occasionally returns a list
        ot = b.get("output_table")
        if isinstance(ot, list):
            b["output_table"] = ot[0] if ot else ""
        elif ot is None:
            b["output_table"] = ""

    # reporting_blocks
    for b in result.get("reporting_blocks", []):
        for field in ("description", "note"):
            if b.get(field) is None:
                b[field] = ""

    return result


def analyzer_node(state: GraphState) -> dict:
    """Preprocess SAS code, then extract structured metadata via LLM."""
    print("\n[ANALYZER] Preprocessing SAS script...")
    clean_code, ingestion_blocks = preprocess_sas(state["sas_code_raw"])

    print(f"  Stripped {len(ingestion_blocks)} ingestion/reporting blocks")
    if not clean_code.strip():
        return {
            "status": "error",
            "error": "No transformation logic found after stripping ingestion/reporting blocks.",
            "sas_code_clean": "",
            "ingestion_blocks": ingestion_blocks,
        }

    print("[ANALYZER] Analyzing transformation logic via LLM...")
    user_prompt = ANALYZER_USER.format(sas_code=clean_code)
    result = call_llm(ANALYZER_SYSTEM, user_prompt, step_name="analyzer")

    # Normalise schema field name
    for key in ["source_tables", "intermediate_tables", "output_tables"]:
        for t in result.get(key, []):
            if "schema" in t and "schema_name" not in t:
                t["schema_name"] = t.pop("schema")

    # Coerce nulls before Pydantic validation
    result = _coerce_nulls(result)

    analysis = SASAnalysis(**result)

    print(f"  Source tables      : {[t.table for t in analysis.source_tables]}")
    print(f"  Intermediate tables: {[t.table for t in analysis.intermediate_tables]}")
    print(f"  Output tables      : {[t.table for t in analysis.output_tables]}")
    print(f"  Constructs         : {analysis.constructs}")
    print(f"  Transformation blocks: {len(analysis.transformation_blocks)}")
    print(f"  Reporting blocks   : {len(analysis.reporting_blocks)}")
    print(f"  Summary            : {analysis.logic_summary[:150]}...")

    log_step("analyzer_output", analysis)
    log_step("ingestion_blocks", ingestion_blocks, is_pydantic=False)

    return {
        "sas_code_clean":   clean_code,
        "ingestion_blocks": ingestion_blocks,
        "analysis":         analysis,
        "status":           "analyzed",
    }