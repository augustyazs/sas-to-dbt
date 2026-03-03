from state.graph_state import GraphState
from models.schemas import SASAnalysis
from tools.llm_client import call_llm
from tools.sas_preprocessor import preprocess_sas
from config.prompts import ANALYZER_SYSTEM, ANALYZER_USER
from utils.logger import log_step


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

    for key in ["source_tables", "intermediate_tables", "output_tables"]:
        for t in result.get(key, []):
            if "schema" in t and "schema_name" not in t:
                t["schema_name"] = t.pop("schema")

    analysis = SASAnalysis(**result)

    print(f"  Source tables: {[t.table for t in analysis.source_tables]}")
    print(f"  Intermediate tables: {[t.table for t in analysis.intermediate_tables]}")
    print(f"  Output tables: {[t.table for t in analysis.output_tables]}")
    print(f"  Constructs: {analysis.constructs}")
    print(f"  Transformation blocks: {len(analysis.transformation_blocks)}")
    print(f"  Reporting blocks: {len(analysis.reporting_blocks)}")
    print(f"  Summary: {analysis.logic_summary[:150]}...")

    log_step("analyzer_output", analysis)
    log_step("ingestion_blocks", ingestion_blocks, is_pydantic=False)

    return {
        "sas_code_clean": clean_code,
        "ingestion_blocks": ingestion_blocks,
        "analysis": analysis,
        "status": "analyzed",
    }