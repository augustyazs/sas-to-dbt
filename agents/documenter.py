from state.graph_state import GraphState
from tools.llm_client import call_llm_text
from config.prompts import DOCUMENTER_SYSTEM, DOCUMENTER_USER
from config.settings import DOC_OUTPUT_DIR
from utils.dbt_writer import write_sas_documentation


def documenter_node(state: GraphState) -> dict:
    """Generate plain-English FDD-style business documentation from SAS analysis."""
    print("\n[DOCUMENTER] Generating business documentation...")

    analysis         = state["analysis"]
    clean_code       = state["sas_code_clean"]
    ingestion_blocks = state.get("ingestion_blocks", [])

    doc_prompt = DOCUMENTER_USER.format(
        sas_code=clean_code,
        analysis_summary=analysis.model_dump_json(indent=2),
        ingestion_blocks=ingestion_blocks if ingestion_blocks else "None",
    )

    sas_documentation = call_llm_text(
        DOCUMENTER_SYSTEM, doc_prompt, step_name="documenter_agent"
    )

    doc_path = write_sas_documentation(sas_documentation, DOC_OUTPUT_DIR)
    print(f"  ✓ Documentation written: {doc_path}")

    return {"sas_documentation": sas_documentation}