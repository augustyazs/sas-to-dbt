from pathlib import Path
from state.graph_state import GraphState
from tools.llm_client import call_llm
from config.prompts import STTM_SYSTEM, STTM_USER
from utils.dbt_writer import write_sttm_excel


def sttm_node(state: GraphState) -> dict:
    """Generate Source-to-Target Mapping Excel from pipeline analysis."""
    print("\n[STTM] Generating Source-to-Target Mapping...")

    analysis  = state["analysis"]
    resolved  = state.get("resolved_mappings")
    doc_dir   = Path(state.get("doc_output_dir", "outputs/documentation"))

    # Skip if no output tables — nothing meaningful to map
    if not analysis.output_tables:
        print("  No output tables found — skipping STTM generation.")
        return {"sttm_data": {}}

    resolved_json = resolved.model_dump_json(indent=2) if resolved else "{}"

    sttm_data = call_llm(
        STTM_SYSTEM,
        STTM_USER.format(
            analysis_json          = analysis.model_dump_json(indent=2),
            resolved_mappings_json = resolved_json,
            logic_summary          = analysis.logic_summary,
        ),
        step_name="sttm_generator_agent",
    )

    # Guard against empty tabs from LLM
    if not sttm_data.get("tabs"):
        print("  STTM returned no tabs — skipping Excel write.")
        return {"sttm_data": sttm_data}

    sttm_path = write_sttm_excel(sttm_data, doc_dir)
    print(f"  STTM written: {sttm_path}")

    return {"sttm_data": sttm_data}