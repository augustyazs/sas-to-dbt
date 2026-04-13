"""
batch_run.py — runs the pipeline over every script in INPUT_SCRIPTS_DIR
               with target platform fixed to pyspark.

Usage:
    python batch_run.py

Delete this file once batch processing is no longer needed.
The default interactive main.py is unaffected.
"""

from pathlib import Path
from models.schemas import DbtConventions
from config.settings import INPUT_SCRIPTS_DIR, COLUMN_MAPPING_DIR, SUPPORTED_TARGETS, get_output_dirs
from utils.file_loader import load_source_script, load_column_mapping
from utils.logger import reset_logs, write_cost_summary
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from graph.builder import build_graph

TARGET_PLATFORM = "pyspark"
SUPPORTED_EXTENSIONS = [".sas", ".txt", ".py", ".scala", ".r", ".sql",
                        ".pls", ".pkb", ".pks", ".prc", ".fnc", ".trg",
                        ".xml"]   # Informatica PowerCenter/PowerMart mappings


def find_column_mapping(script_path: Path) -> Path | None:
    stem = script_path.stem
    for ext in [".json", ".csv"]:
        candidate = COLUMN_MAPPING_DIR / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    return None


def run_single(script_file: Path, graph) -> dict:
    """Run the pipeline for one script. Returns final state."""
    print(f"\n{'=' * 60}")
    print(f"Processing : {script_file.name}")
    print(f"Target     : {TARGET_PLATFORM.upper()}")
    print(f"{'=' * 60}")

    source_code  = load_source_script(script_file)
    script_stem  = script_file.stem
    mapping_path = find_column_mapping(script_file)

    if not mapping_path:
        print(f"  SKIP: No column mapping found for '{script_stem}' — skipping.")
        return {"status": "skipped", "script": script_file.name}

    try:
        col_mappings = load_column_mapping(mapping_path)
    except ValueError as e:
        print(f"  SKIP: Invalid column mapping '{mapping_path.name}' — {e}")
        return {"status": "skipped", "script": script_file.name}

    if not col_mappings:
        print(f"  SKIP: Column mapping '{mapping_path.name}' is empty — skipping.")
        return {"status": "skipped", "script": script_file.name}

    dirs        = get_output_dirs(script_stem)
    outputs_dir = dirs["outputs"]
    doc_dir     = dirs["docs"]
    logs_dir    = dirs["logs"]

    print(f"  Mapping : {mapping_path.name} ({len(col_mappings)} entries)")
    print(f"  Output  : {outputs_dir}")

    reset_usage()
    reset_logs(logs_dir)

    initial_state = {
        "sas_code_raw":    source_code,
        "source_filename": script_file.name,
        "target_platform": TARGET_PLATFORM,
        "column_mappings": col_mappings,
        "conventions":     DbtConventions(),
        "review_count":    0,
        "status":          "started",
        "outputs_dir":     str(outputs_dir),
        "doc_output_dir":  str(doc_dir),
        "logs_dir":        str(logs_dir),
    }

    final_state = graph.invoke(initial_state)

    usage  = get_usage_log()
    totals = get_total_cost()
    write_cost_summary(usage, totals)

    print(f"\n  Status : {final_state.get('status', 'unknown')}")
    print(f"  Cost   : ${totals['total_cost_usd']:.4f} | "
          f"Calls: {totals['calls']} | "
          f"Time: {totals['total_response_time_seconds']:.1f}s")

    return final_state


def main():
    if not INPUT_SCRIPTS_DIR.exists():
        print(f"ERROR: Input directory not found: {INPUT_SCRIPTS_DIR}")
        return

    scripts = sorted([
        f for f in INPUT_SCRIPTS_DIR.iterdir()
        if f.suffix.lower() in SUPPORTED_EXTENSIONS
    ])

    if not scripts:
        print(f"No supported scripts found in {INPUT_SCRIPTS_DIR}")
        return

    print(f"Found {len(scripts)} script(s) in {INPUT_SCRIPTS_DIR}")
    print(f"Target platform : {TARGET_PLATFORM.upper()}")

    graph   = build_graph()
    results = []

    for script_file in scripts:
        result = run_single(script_file, graph)
        results.append({
            "script": script_file.name,
            "status": result.get("status", "unknown"),
        })

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("BATCH SUMMARY")
    print(f"{'=' * 60}")
    for r in results:
        status = r["status"]
        icon   = "✓" if status == "done" else "~" if status == "skipped" else "✗"
        print(f"  {icon}  {r['script']:40s}  {status}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()