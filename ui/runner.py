import time
import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs
from ui.components import render_pipeline_progress


STEP_ORDER = [
    "analyzer",
    "resolver",
    "architect",
    "generator",
    "write_output",
    "documenter",
    "sttm",
]

STEP_LABELS = {
    "analyzer":     "Analyzer",
    "resolver":     "Resolver",
    "architect":    "Architect",
    "generator":    "Developer",
    "write_output": "Write Output",
    "documenter":   "Documenter",
    "sttm":         "STTM Generator",
}

STEP_SECTION = {
    "analyzer":  "agents",
    "resolver":  "agents",
    "architect": "agents",
    "generator": "agents",
    "documenter": "documents",
    "sttm":       "documents",
    # write_output is handled separately as the footer bar
}


def _fmt_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}m {s:.0f}s"


def run_pipeline(
    sas_code: str,
    mappings: list[ColumnMapping],
    conventions: DbtConventions,
    progress_slot,
):
    reset_usage()
    reset_logs()
    graph = build_graph()

    initial_state = {
        "sas_code_raw":    sas_code,
        "column_mappings": mappings,
        "conventions":     conventions,
        "review_count":    0,
        "status":          "started",
    }

    # Build initial steps — fixed agent + document steps, all pending
    steps: list[dict] = []
    for s in STEP_ORDER:
        if s == "write_output":
            continue  # rendered as separate bar, not a timeline row
        steps.append({
            "key":     s,
            "label":   STEP_LABELS[s],
            "section": STEP_SECTION[s],
            "status":  "pending",
            "elapsed": None,
        })

    st.session_state.pipeline_steps = list(steps)
    st.session_state.write_output_status = wo["status"]
    render_pipeline_progress(progress_slot, steps, wo["status"])

    step_start_times: dict[str, float] = {}
    final_state: dict = {}

    # ── helpers ────────────────────────────────────────────────────────────────

    def _refresh():
        st.session_state.pipeline_steps = list(steps)
        st.session_state.write_output_status = wo["status"]
        render_pipeline_progress(progress_slot, steps, wo["status"])

    def _step_by_key(key: str) -> dict | None:
        return next((s for s in steps if s["key"] == key), None)

    def _set_running(key: str):
        step_start_times[key] = time.perf_counter()
        s = _step_by_key(key)
        if s:
            s["status"]  = "running"
            s["elapsed"] = None
        _refresh()

    def _set_done(key: str, errored: bool = False):
        elapsed = time.perf_counter() - step_start_times.get(key, time.perf_counter())
        s = _step_by_key(key)
        if s:
            s["status"]  = "error" if errored else "done"
            s["elapsed"] = elapsed
        _refresh()

    def _add_review_row(label: str, status: str):
        step_start_times[label] = time.perf_counter()
        steps.append({
            "key":     label,
            "label":   label,
            "section": "review",
            "status":  status,
            "elapsed": None,
        })
        _refresh()

    def _finish_review_row(label: str, status: str):
        elapsed = time.perf_counter() - step_start_times.get(label, time.perf_counter())
        s = _step_by_key(label)
        if s:
            s["status"]  = status
            s["elapsed"] = elapsed
        _refresh()

    # ── kick off first step ────────────────────────────────────────────────────
    _set_running("analyzer")

    # track which reviewer/fixer pass we're on
    current_reviewer_label: str | None = None
    current_fixer_label:    str | None = None
    # use a mutable container so inner logic can update write_output_status
    wo = {"status": "pending"}

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                final_state.update(node_output)

                # ── Reviewer ──────────────────────────────────────────────────
                if node_name == "reviewer":
                    count  = node_output.get("review_count", 1)
                    passed = node_output.get("status", "") in ("complete", "complete_with_warnings")
                    label  = f"Reviewer {count}"

                    # If this reviewer row is already in the list, finish it
                    if _step_by_key(label):
                        _finish_review_row(label, "done" if passed else "error")
                    else:
                        # First time we see this reviewer — add + immediately finish
                        _add_review_row(label, "running")
                        _finish_review_row(label, "done" if passed else "error")

                    current_reviewer_label = label

                    if not passed:
                        # Fixer is next
                        fixer_label = f"Fixer {count}"
                        current_fixer_label = fixer_label
                        _add_review_row(fixer_label, "running")
                    else:
                        # Move to write_output
                        wo["status"] = "running"
                        _refresh()

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    if current_fixer_label:
                        _finish_review_row(current_fixer_label, "done")
                    # Next reviewer pass will be added when reviewer fires again

                # ── write_output ───────────────────────────────────────────────
                elif node_name == "write_output":
                    wo["status"] = "done"
                    _refresh()
                    _set_running("documenter")

                # ── all other fixed linear steps ───────────────────────────────
                else:
                    errored = node_output.get("status") in ("error", "halted")
                    _set_done(node_name, errored=errored)

                    # Advance next pending fixed step
                    fixed_keys = [s["key"] for s in steps if s["section"] in ("agents", "documents")]
                    for key in fixed_keys:
                        obj = _step_by_key(key)
                        if obj and obj["status"] == "pending":
                            _set_running(key)
                            break

                    # After generator finishes, add Reviewer 1 as running
                    if node_name == "generator":
                        _add_review_row("Reviewer 1", "running")
                        current_reviewer_label = "Reviewer 1"

    except Exception as e:
        st.error(f"Pipeline error: {str(e)}")
        return None, None

    st.session_state.pipeline_steps = list(steps)
    st.session_state.write_output_status = wo["status"]
    render_pipeline_progress(progress_slot, steps, wo["status"])

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}