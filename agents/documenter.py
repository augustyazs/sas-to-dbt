from pathlib import Path
from state.graph_state import GraphState
from tools.llm_client import call_llm_text
from config.prompts import DOCUMENTER_SYSTEM, DOCUMENTER_USER
from utils.dbt_writer import write_sas_documentation


def documenter_node(state: GraphState) -> dict:
    """Generate plain-English FDD-style documentation from source analysis."""
    print("\n[DOCUMENTER] Generating documentation...")

    analysis        = state["analysis"]
    clean_code      = state["sas_code_clean"]
    ingestion_blocks= state.get("ingestion_blocks", [])
    source_language = state.get("detected_language", "SAS")
    target_platform = state.get("target_platform", "dbt")
    doc_dir         = Path(state.get("doc_output_dir", "outputs/documentation"))

    doc_prompt = DOCUMENTER_USER.format(
        source_language  = source_language,
        target_platform  = target_platform,
        source_code      = clean_code,
        analysis_summary = analysis.model_dump_json(indent=2),
        ingestion_blocks = ingestion_blocks if ingestion_blocks else "None",
    )

    documentation = call_llm_text(
        DOCUMENTER_SYSTEM, doc_prompt, step_name="documenter_agent"
    )

    doc_path = write_sas_documentation(documentation, doc_dir)
    print(f"  Documentation written: {doc_path}")

    return {"sas_documentation": documentation}