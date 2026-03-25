import sys
from pathlib import Path
from config.settings import SAS_SCRIPTS_DIR, COLUMN_MAPPING_PATH, DBT_CONVENTIONS_PATH
from utils.file_loader import load_sas_script, load_all_sas_scripts, load_column_mapping, load_conventions
from utils.logger import log_step, reset_logs, write_cost_summary
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from graph.builder import build_graph


def run(sas_path: str | None = None):
    """Run the SAS-to-dbt conversion graph."""
    print("=" * 60)
    print("SAS → dbt Agentic Conversion Pipeline (LangGraph)")
    print("=" * 60)

    print("\n[LOAD] Loading inputs...")
    if sas_path:
        sas_code    = load_sas_script(Path(sas_path))
        script_name = Path(sas_path).name
    else:
        scripts = load_all_sas_scripts(SAS_SCRIPTS_DIR)
        if not scripts:
            print("  ERROR: No .sas files found in inputs/sas_scripts/")
            return
        script_name = list(scripts.keys())[0]
        sas_code    = scripts[script_name]
        if len(scripts) > 1:
            print(f"  Found {len(scripts)} scripts. Processing first: {script_name}")
            print(f"  To process a specific script: python main.py <path_to_script.sas>")

    col_mappings = load_column_mapping(COLUMN_MAPPING_PATH)
    conventions  = load_conventions(DBT_CONVENTIONS_PATH)

    print(f"  SAS script    : {script_name} ({len(sas_code)} chars)")
    print(f"  Column mappings: {len(col_mappings)} entries")
    print(f"  Conventions   : {conventions.target_dialect}, max {conventions.max_joins_per_model} joins/model")

    reset_usage()
    reset_logs()

    print("\n[GRAPH] Building and running pipeline...")
    graph = build_graph()

    initial_state = {
        "sas_code_raw":    sas_code,
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
            f"  {entry['step']:30s} | "
            f"in: {entry['input_tokens']:>7,} | "
            f"out: {entry['output_tokens']:>7,} | "
            f"${entry['cost_usd']:.4f} | "
            f"{entry['response_time_seconds']:.1f}s"
        )
    print(f"  {'-' * 58}")
    print(
        f"  {'TOTAL':30s} | "
        f"in: {totals['total_input_tokens']:>7,} | "
        f"out: {totals['total_output_tokens']:>7,} | "
        f"${totals['total_cost_usd']:.4f} | "
        f"{totals['total_response_time_seconds']:.1f}s"
    )
    print(f"  LLM calls: {totals['calls']}")

    log_step("cost_summary", totals, is_pydantic=False)
    write_cost_summary(usage, totals)

    print(f"\n{'=' * 60}")
    print(f"Final status: {final_state.get('status', 'unknown')}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    script_path = sys.argv[1] if len(sys.argv) > 1 else None
    run(script_path)