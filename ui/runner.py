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
    "generator":    "Generator",
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
}

# Agent numbers: fixed for agents + documents; reviewer/fixer always show 5/6
AGENT_NUMBERS = {
    "analyzer":  1,
    "resolver":  2,
    "architect": 3,
    "generator": 4,
    "documenter": 7,
    "sttm":       8,
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

    # Build initial steps — agents first, then reviewer/fixer placeholder rows,
    # then documents. All pending to start.
    steps: list[dict] = []

    # Agent steps (1-4)
    for s in ["analyzer", "resolver", "architect", "generator"]:
        steps.append({
            "key":        s,
            "label":      STEP_LABELS[s],
            "section":    "agents",
            "agent_num":  AGENT_NUMBERS[s],
            "status":     "pending",
            "elapsed":    None,
        })

    # Reviewer/Fixer section — starts empty but section header shows immediately.
    # We'll append rows here dynamically as reviewer/fixer nodes fire.

    # Document steps (7-8)
    for s in ["documenter", "sttm"]:
        steps.append({
            "key":        s,
            "label":      STEP_LABELS[s],
            "section":    "documents",
            "agent_num":  AGENT_NUMBERS[s],
            "status":     "pending",
            "elapsed":    None,
        })

    wo = {"status": "pending"}

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

    def _add_review_row(label: str, status: str, agent_num: int):
        step_start_times[label] = time.perf_counter()
        # Insert review rows BEFORE the documents section
        insert_idx = next(
            (i for i, s in enumerate(steps) if s["section"] == "documents"),
            len(steps),
        )
        steps.insert(insert_idx, {
            "key":       label,
            "label":     label,
            "section":   "review",
            "agent_num": agent_num,
            "status":    status,
            "elapsed":   None,
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

    current_fixer_label: str | None = None

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

                    if _step_by_key(label):
                        _finish_review_row(label, "done" if passed else "error")
                    else:
                        _add_review_row(label, "running", agent_num=5)
                        _finish_review_row(label, "done" if passed else "error")

                    if not passed:
                        fixer_label = f"Fixer {count}"
                        current_fixer_label = fixer_label
                        _add_review_row(fixer_label, "running", agent_num=6)
                    else:
                        wo["status"] = "running"
                        _refresh()

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    if current_fixer_label:
                        _finish_review_row(current_fixer_label, "done")
                        current_fixer_label = None

                # ── write_output ───────────────────────────────────────────────
                elif node_name == "write_output":
                    # Don't mark wo as done yet — wait for sttm to complete
                    wo["status"] = "running"
                    _refresh()
                    _set_running("documenter")

                # ── all other fixed linear steps ───────────────────────────────
                else:
                    errored = node_output.get("status") in ("error", "halted")
                    _set_done(node_name, errored=errored)

                    # Advance next pending fixed step (agents + documents)
                    fixed_keys = [
                        s["key"] for s in steps
                        if s["section"] in ("agents", "documents")
                    ]
                    for key in fixed_keys:
                        obj = _step_by_key(key)
                        if obj and obj["status"] == "pending":
                            _set_running(key)
                            break

                    # After generator, prime Reviewer 1 as running
                    if node_name == "generator":
                        _add_review_row("Reviewer 1", "running", agent_num=5)

                    # After sttm (last step), mark Write Output bar as done
                    if node_name == "sttm":
                        wo["status"] = "done"
                        _refresh()

    except Exception as e:
        st.error(f"Pipeline error: {str(e)}")
        return None, None

    # Final render
    st.session_state.pipeline_steps = list(steps)
    st.session_state.write_output_status = wo["status"]
    render_pipeline_progress(progress_slot, steps, wo["status"])

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}