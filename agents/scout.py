from pathlib import Path
from state.graph_state import GraphState
from tools.llm_client import call_llm
from tools.language_detector import detect_language
from config.prompts import SCOUT_SYSTEM, SCOUT_USER
from utils.logger import log_step


# ── Scout node ────────────────────────────────────────────────────────────────

def scout_node(state: GraphState) -> dict:
    """
    Detects source language and generates per-run input/output conventions.
    Runs before everything else. Uses LLM only when deterministic detection fails.
    """
    print("\n[SCOUT] Detecting language and generating conventions...")

    raw_code        = state["sas_code_raw"]
    target_platform = state.get("target_platform", "dbt")
    filename        = state.get("source_filename", "")

    # ── Step 1: deterministic detection ──────────────────────────────────────
    detected = detect_language(Path(filename) if filename else Path("unknown.txt"), raw_code)

    if detected:
        print(f"  Language detected deterministically: {detected}")
    else:
        print("  Ambiguous — falling back to LLM detection")

    # ── Step 2: LLM call (always for conventions; detection bundled if needed) ─
    user_prompt = SCOUT_USER.format(
        source_code=raw_code[:12000],   # cap to avoid blowing context on huge scripts
        target_platform=target_platform,
        detected_language=detected or "UNKNOWN — detect from code",
    )

    result = call_llm(SCOUT_SYSTEM, user_prompt, step_name="scout")

    # If deterministic detection succeeded, trust it over LLM
    final_language = detected if detected else result.get("detected_source_language", "Unknown")

    input_conv  = result.get("input_conventions", {})
    output_conv = result.get("output_conventions", {})

    print(f"  Source language  : {final_language}")
    print(f"  Target platform  : {target_platform}")
    print(f"  Input conv items : {sum(len(v) for v in input_conv.values() if isinstance(v, list))}")
    print(f"  Output conv items: {sum(len(v) for v in output_conv.values() if isinstance(v, list))}")

    log_step("scout_output", result, is_pydantic=False)

    return {
        "detected_language":  final_language,
        "input_conventions":  input_conv,
        "output_conventions": output_conv,
        "status": "scouted",
    }