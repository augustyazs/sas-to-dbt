from pathlib import Path
from state.graph_state import GraphState
from tools.llm_client import call_llm
from tools.language_detector import detect_language
from config.prompts import SCOUT_SYSTEM, SCOUT_USER, SCOUT_VALIDITY_SYSTEM, SCOUT_VALIDITY_USER
from utils.logger import log_step


# ── Scout node ────────────────────────────────────────────────────────────────

def _check_validity(raw_code: str) -> tuple[bool, str]:
    """
    Dedicated validity-only LLM call — isolated from the full Scout analysis
    so the model cannot conflate 'no SQL verbs' with 'still worth analyzing'.
    Returns (is_valid, reason).
    """
    result = call_llm(
        SCOUT_VALIDITY_SYSTEM,
        SCOUT_VALIDITY_USER.format(source_code=raw_code[:4000]),
        step_name="scout_validity",
    )
    is_valid = result.get("is_valid_pipeline", False)
    reason   = result.get("reason", "No transformation logic detected.")
    return bool(is_valid), reason


def scout_node(state: GraphState) -> dict:
    """
    Detects source language and generates per-run input/output conventions.
    Runs before everything else. Halts pipeline if input is not a valid script.
    """
    print("\n[SCOUT] Detecting language and generating conventions...")

    raw_code        = state["sas_code_raw"]
    target_platform = state.get("target_platform", "dbt")
    filename        = state.get("source_filename", "")

    # ── Step 1: deterministic language detection ──────────────────────────────
    detected = detect_language(Path(filename) if filename else Path("unknown.txt"), raw_code)
    if detected:
        print(f"  Language detected deterministically: {detected}")
    else:
        print("  Ambiguous — falling back to LLM detection")

    # ── Step 2: validity check (only when deterministic detection failed) ─────
    if not detected:
        print("  Checking input validity...")
        is_valid, reason = _check_validity(raw_code)
        if not is_valid:
            print(f"\n  ERROR: Invalid input — {reason}")
            print("  Pipeline cannot proceed. Please provide a valid transformation script.")
            log_step("scout_output", {"is_valid_pipeline": False, "reason": reason}, is_pydantic=False)
            return {
                "status": "error",
                "error":  f"Invalid input: {reason}",
            }

    # ── Step 3: full Scout analysis ───────────────────────────────────────────
    user_prompt = SCOUT_USER.format(
        source_code       = raw_code[:12000],
        target_platform   = target_platform,
        detected_language = detected or "UNKNOWN — detect from code",
    )

    result = call_llm(SCOUT_SYSTEM, user_prompt, step_name="scout")
    log_step("scout_output", result, is_pydantic=False)

    final_language = detected if detected else result.get("detected_source_language", "Unknown")
    input_conv     = result.get("input_conventions", {})
    output_conv    = result.get("output_conventions", {})

    print(f"  Source language  : {final_language}")
    print(f"  Target platform  : {target_platform}")
    print(f"  Input conv items : {sum(len(v) for v in input_conv.values() if isinstance(v, list))}")
    print(f"  Output conv items: {sum(len(v) for v in output_conv.values() if isinstance(v, list))}")

    return {
        "detected_language":  final_language,
        "input_conventions":  input_conv,
        "output_conventions": output_conv,
        "status": "scouted",
    }