import re
from state.graph_state import GraphState
from tools.llm_client import call_llm
from config.prompts import SCOUT_SYSTEM, SCOUT_USER
from utils.logger import log_step


# ── Deterministic language detection ─────────────────────────────────────────

_EXT_MAP = {
    ".sas":   "SAS",
    ".py":    "Python",
    ".scala": "Scala",
    ".r":     "R",
    ".sql":   "SQL",
}

# Keyword fingerprints — ordered most-specific first
_KEYWORD_SIGNATURES: list[tuple[str, list[str]]] = [
    ("SAS",        [r"\bproc\s+sql\b", r"\bdata\s+\w+\s*;", r"\b%macro\b", r"\blibname\b", r"\brun\s*;", r"\bquit\s*;"]),
    ("PySpark",    [r"\bSparkSession\b", r"\bspark\.read\b", r"\.withColumnRenamed\b", r"\bpyspark\b", r"from\s+pyspark"]),
    ("Scala",      [r"\bval\s+\w+\s*[:=]", r"\bSparkSession\b", r"import\s+org\.apache\.spark", r"\.toDF\b"]),
    ("R",          [r"\blibrary\s*\(", r"\bdata\.frame\b", r"\bggplot\b", r"\bdplyr\b", r"\btibble\b"]),
    ("Informatica",[r"\bSource Qualifier\b", r"\bTarget Definition\b", r"\bExpression Transformation\b"]),
    ("PL/SQL",     [r"\bBEGIN\b", r"\bEXCEPTION\b", r"\bEND\s*;", r"\bCURSOR\b", r"\bBULK\s+COLLECT\b"]),
    ("SQL",        [r"\bSELECT\b", r"\bFROM\b", r"\bWHERE\b", r"\bCREATE\s+TABLE\b"]),
]

_SHEBANG_MAP = {
    "python": "Python",
    "scala":  "Scala",
    "rscript":"R",
    "r":      "R",
}


def detect_language_deterministic(code: str, filename: str | None = None) -> str | None:
    """
    Returns detected language string or None if ambiguous.
    Order: file extension → shebang → keyword heuristics.
    """
    # 1. File extension
    if filename:
        ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext in _EXT_MAP:
            return _EXT_MAP[ext]

    # 2. Shebang line
    first_line = code.strip().splitlines()[0].lower() if code.strip() else ""
    if first_line.startswith("#!"):
        for key, lang in _SHEBANG_MAP.items():
            if key in first_line:
                return lang

    # 3. Keyword heuristics — require at least 2 matching patterns to avoid false positives
    sample = code[:8000]  # only scan first 8k chars for speed
    scores: dict[str, int] = {}
    for lang, patterns in _KEYWORD_SIGNATURES:
        hits = sum(1 for p in patterns if re.search(p, sample, re.IGNORECASE))
        if hits >= 2:
            scores[lang] = hits

    if scores:
        return max(scores, key=lambda k: scores[k])

    return None  # genuinely ambiguous — hand off to LLM


# ── Scout node ────────────────────────────────────────────────────────────────

def scout_node(state: GraphState) -> dict:
    """
    Detects source language and generates per-run input/output conventions.
    Runs before everything else. Uses LLM only when deterministic detection fails.
    """
    print("\n[SCOUT] Detecting language and generating conventions...")

    raw_code       = state["sas_code_raw"]
    target_platform = state.get("target_platform", "dbt")
    filename       = state.get("source_filename")  # optional, set by loader

    # ── Step 1: deterministic detection ──────────────────────────────────────
    detected = detect_language_deterministic(raw_code, filename)

    if detected:
        print(f"  Language detected deterministically: {detected}")
        llm_needed = False
    else:
        print("  Ambiguous — falling back to LLM detection")
        llm_needed = True

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