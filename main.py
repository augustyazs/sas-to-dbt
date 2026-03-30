import sys
from pathlib import Path
from config.settings import (
    SAS_SCRIPTS_DIR, COLUMN_MAPPING_PATH, DBT_CONVENTIONS_PATH,
    SUPPORTED_TARGETS,
)
from models.schemas import DbtConventions
from utils.file_loader import load_sas_script, load_column_mapping, load_conventions
from utils.logger import log_step, reset_logs, write_cost_summary
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from graph.builder import build_graph


def prompt_target_platform() -> str:
    """Interactively ask the user for the target platform."""
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
        print(f"  Invalid choice. Pick from: {options}")


def find_source_file(directory: Path) -> Path | None:
    """Find first supported source file in directory (.sas, .txt, .py, .scala, .r, .sql)."""
    extensions = [".sas", ".txt", ".py", ".scala", ".r", ".sql"]
    for ext in extensions:
        matches = sorted(directory.glob(f"*{ext}"))
        if matches:
            return matches[0]
    return None


def run(source_path: str | None = None, target_platform: str | None = None):
    """Run the migration pipeline."""

    # ── Target platform ───────────────────────────────────────────────────────
    if target_platform:
        target_platform = target_platform.lower()
        if target_platform not in SUPPORTED_TARGETS:
            print(f"  ERROR: Unsupported target '{target_platform}'. "
                  f"Choose from: {sorted(SUPPORTED_TARGETS)}")
            return
    else:
        target_platform = prompt_target_platform()

    print("=" * 60)
    print(f"Migration Pipeline — target: {target_platform.upper()}")
    print("=" * 60)

    # ── Source file ───────────────────────────────────────────────────────────
    print("\n[LOAD] Loading inputs...")
    if source_path:
        fp = Path(source_path)
    else:
        fp = find_source_file(SAS_SCRIPTS_DIR)
        if not fp:
            print(f"  ERROR: No source files found in {SAS_SCRIPTS_DIR}")
            print(f"  Supported extensions: .sas .txt .py .scala .r .sql")
            return

    source_code  = load_sas_script(fp)      # handles encoding fallbacks
    script_name  = fp.name

    col_mappings = load_column_mapping(COLUMN_MAPPING_PATH)
    # dbt_conventions.json only needed for the dbt path — Scout handles all others
    conventions  = load_conventions(DBT_CONVENTIONS_PATH) if target_platform == "dbt" else DbtConventions()

    print(f"  Source file     : {script_name} ({len(source_code):,} chars)")
    print(f"  Column mappings : {len(col_mappings)} entries")
    print(f"  Target platform : {target_platform}")

    reset_usage()
    reset_logs()

    graph = build_graph()

    initial_state = {
        "sas_code_raw":    source_code,
        "source_filename": script_name,
        "target_platform": target_platform,
        "column_mappings": col_mappings,
        "conventions":     conventions,
        "review_count":    0,
        "status":          "started",
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
    # Usage:
    #   python main.py                              — prompts for target, auto-finds source file
    #   python main.py inputs/.../file.txt          — prompts for target, uses given file
    #   python main.py inputs/.../file.txt pyspark  — no prompts, fully specified
    source_arg = sys.argv[1] if len(sys.argv) > 1 else None
    target_arg = sys.argv[2] if len(sys.argv) > 2 else None
    run(source_arg, target_arg)