from state.graph_state import GraphState
from tools.llm_client import call_llm
from config.prompts import STTM_SYSTEM, STTM_USER
from config.settings import DOC_OUTPUT_DIR
from utils.dbt_writer import write_sttm_excel


def sttm_node(state: GraphState) -> dict:
    """Generate Source-to-Target Mapping Excel from SAS analysis."""
    print("\n[STTM GENERATOR] Generating Source-to-Target Mapping...")

    analysis = state["analysis"]
    resolved = state.get("resolved_mappings")
    resolved_json = resolved.model_dump_json(indent=2) if resolved else "{}"

    sttm_prompt = STTM_USER.format(
        analysis_json=analysis.model_dump_json(indent=2),
        resolved_mappings_json=resolved_json,
        logic_summary=analysis.logic_summary,
    )

    sttm_data = call_llm(STTM_SYSTEM, sttm_prompt, step_name="sttm_generator_agent")

    sttm_path = write_sttm_excel(sttm_data, DOC_OUTPUT_DIR)
    print(f"  ✓ STTM written: {sttm_path}")

    return {"sttm_data": sttm_data}