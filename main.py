import sys
from pathlib import Path
from models.schemas import DbtConventions
from config.settings import (
    INPUT_SCRIPTS_DIR,
    COLUMN_MAPPING_DIR,
    SUPPORTED_TARGETS,
    get_output_dirs,
)
from utils.file_loader import load_source_script, load_column_mapping
from utils.logger import reset_logs, write_cost_summary
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from graph.builder import build_graph


SUPPORTED_EXTENSIONS = [".sas", ".txt", ".py", ".scala", ".r", ".sql",
                        ".pls", ".pkb", ".pks", ".prc", ".fnc", ".trg",
                        ".xml"]   # Informatica PowerCenter/PowerMart mappings


def prompt_target_platform() -> str:
    options = sorted(SUPPORTED_TARGETS)
    print("\nSupported target platforms:")
    for i, opt in enumerate(options, 1):
        print(f"  {i}. {opt}")
    while True:
        choice = input("\nEnter target platform (or number): ").strip().lower()
        if choice in SUPPORTED_TARGETS:
            return choice
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]
        print(f"  Invalid choice. Options: {options}")


def list_available_scripts() -> list[Path]:
    """Return all supported source files found in INPUT_SCRIPTS_DIR."""
    if not INPUT_SCRIPTS_DIR.exists():
        return []
    files = []
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(INPUT_SCRIPTS_DIR.glob(f"*{ext}"))
    return sorted(files)


def prompt_source_script() -> Path | None:
    """
    Let the user pick a script from INPUT_SCRIPTS_DIR.
    If only one file exists, select it automatically.
    """
    files = list_available_scripts()

    if not files:
        print(f"  ERROR: No source files found in {INPUT_SCRIPTS_DIR}")
        print(f"  Supported extensions: {SUPPORTED_EXTENSIONS}")
        return None

    if len(files) == 1:
        print(f"  Auto-selected only available script: {files[0].name}")
        return files[0]

    print("\nAvailable scripts:")
    for i, f in enumerate(files, 1):
        print(f"  {i}. {f.name}")

    while True:
        choice = input("\nEnter script name or number: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return files[int(choice) - 1]
        # Allow partial name match
        matches = [f for f in files if choice.lower() in f.name.lower()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            print(f"  Ambiguous — matched: {[f.name for f in matches]}. Be more specific.")
        else:
            print(f"  Not found. Pick a number or type part of the filename.")


def resolve_column_mapping(script_path: Path) -> Path | None:
    """
    Column mapping file must have the same stem as the input script.
    e.g. glp1.sas → column_mappings/glp1.json or glp1.csv
    Returns None if not found.
    """
    stem = script_path.stem
    for ext in [".json", ".csv"]:
        candidate = COLUMN_MAPPING_DIR / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    return None


def run(source_path: str | None = None, target_platform: str | None = None):

    # ── Target platform ───────────────────────────────────────────────────────
    if target_platform:
        target_platform = target_platform.lower()
        if target_platform not in SUPPORTED_TARGETS:
            print(f"  ERROR: Unsupported target '{target_platform}'. "
                  f"Options: {sorted(SUPPORTED_TARGETS)}")
            return
    else:
        target_platform = prompt_target_platform()

    print("=" * 60)
    print(f"Migration Pipeline — target: {target_platform.upper()}")
    print("=" * 60)

    # ── Source script ─────────────────────────────────────────────────────────
    print("\n[LOAD] Loading inputs...")
    if source_path:
        script_file = Path(source_path)
        if not script_file.exists():
            print(f"  ERROR: File not found: {source_path}")
            return
    else:
        script_file = prompt_source_script()
        if not script_file:
            return

    script_stem = script_file.stem   # e.g. "glp1" from "glp1.sas"
    source_code = load_source_script(script_file)

    # ── Column mapping (same name as script, required) ───────────────────────
    mapping_path = resolve_column_mapping(script_file)
    if not mapping_path:
        print(f"\n  ERROR: No column mapping file found for '{script_file.stem}'.")
        print(f"  Expected: {COLUMN_MAPPING_DIR / script_file.stem}.json or .csv")
        print("  Pipeline cannot proceed without a column mapping. Exiting.")
        return

    col_mappings = load_column_mapping(mapping_path)
    if not col_mappings:
        print(f"\n  ERROR: Column mapping file '{mapping_path.name}' is empty.")
        print("  Pipeline cannot proceed without at least one mapping entry. Exiting.")
        return

    print(f"  Column mapping   : {mapping_path.name} ({len(col_mappings)} entries)")

    # ── Runtime output dirs (per script) ─────────────────────────────────────
    dirs = get_output_dirs(script_stem)
    outputs_dir  = dirs["outputs"]
    doc_dir      = dirs["docs"]
    logs_dir     = dirs["logs"]

    print(f"  Source file     : {script_file.name} ({len(source_code):,} chars)")
    print(f"  Target platform : {target_platform}")
    print(f"  Output dir      : {outputs_dir}")
    print(f"  Logs dir        : {logs_dir}")

    reset_usage()
    reset_logs(logs_dir)   # pass per-script log dir

    graph = build_graph()

    initial_state = {
        "sas_code_raw":    source_code,
        "source_filename": script_file.name,
        "target_platform": target_platform,
        "column_mappings": col_mappings,
        "conventions":     DbtConventions(),   # empty default — Scout overrides
        "review_count":    0,
        "status":          "started",
        # runtime dirs passed through state so agents can use them
        "outputs_dir":     str(outputs_dir),
        "doc_output_dir":  str(doc_dir),
        "logs_dir":        str(logs_dir),
    }

    final_state = graph.invoke(initial_state)

    # ── Cost summary ──────────────────────────────────────────────────────────
    usage  = get_usage_log()
    totals = get_total_cost()

    print(f"\n{'=' * 60}")
    print("COST SUMMARY")
    print(f"{'=' * 60}")
    for entry in usage:
        print(
            f"  {entry['step']:35s} | "
            f"in: {entry['input_tokens']:>7,} | "
            f"out: {entry['output_tokens']:>7,} | "
            f"${entry['cost_usd']:.4f} | "
            f"{entry['response_time_seconds']:.1f}s"
        )
    print(f"  {'-' * 63}")
    print(
        f"  {'TOTAL':35s} | "
        f"in: {totals['total_input_tokens']:>7,} | "
        f"out: {totals['total_output_tokens']:>7,} | "
        f"${totals['total_cost_usd']:.4f} | "
        f"{totals['total_response_time_seconds']:.1f}s"
    )
    print(f"  LLM calls: {totals['calls']}")

    write_cost_summary(usage, totals)

    print(f"\n{'=' * 60}")
    print(f"Final status: {final_state.get('status', 'unknown')}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    # python main.py                                    — fully interactive
    # python main.py inputs/scripts/glp1.sas            — prompts for target
    # python main.py inputs/scripts/glp1.sas pyspark    — no prompts
    source_arg = sys.argv[1] if len(sys.argv) > 1 else None
    target_arg = sys.argv[2] if len(sys.argv) > 2 else None
    run(source_arg, target_arg)