import time
import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs, write_cost_summary
from ui.components import render_pipeline_progress


STEP_ORDER = [
    "analyzer", "resolver", "architect", "generator",
    "write_output", "documenter", "sttm",
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

    # Build initial steps list
    steps: list[dict] = []

    # Agent steps (1–4)
    for s in ["analyzer", "resolver", "architect", "generator"]:
        steps.append({
            "key":       s,
            "label":     STEP_LABELS[s],
            "section":   "agents",
            "agent_num": AGENT_NUMBERS[s],
            "status":    "pending",
            "elapsed":   None,
        })

    # Reviewer 1 is always guaranteed to run — pre-seed as pending
    steps.append({
        "key":       "Reviewer 1",
        "label":     "Reviewer 1",
        "section":   "review",
        "agent_num": 5,
        "status":    "pending",
        "elapsed":   None,
    })

    # Document steps (7–8)
    for s in ["documenter", "sttm"]:
        steps.append({
            "key":       s,
            "label":     STEP_LABELS[s],
            "section":   "documents",
            "agent_num": AGENT_NUMBERS[s],
            "status":    "pending",
            "elapsed":   None,
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

    def _upsert_review_row(label: str, status: str, agent_num: int):
        """Update an existing review row, or insert a new one before documents.
        This prevents duplicate rows regardless of call order.
        """
        existing = _step_by_key(label)
        if existing:
            # Row already exists — just update status and record start time
            step_start_times[label] = time.perf_counter()
            existing["status"]  = status
            existing["elapsed"] = None
        else:
            # Insert before the documents section
            step_start_times[label] = time.perf_counter()
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

    def _close_all_running_review_rows():
        """Force-close any review rows still running (e.g. when graph skips fixer)."""
        for step in steps:
            if step["section"] == "review" and step["status"] == "running":
                elapsed = time.perf_counter() - step_start_times.get(
                    step["key"], time.perf_counter()
                )
                step["status"]  = "done"
                step["elapsed"] = elapsed

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

                    # Finish the existing row (always exists: pre-seeded for 1,
                    # or created by fixer for 2+)
                    _finish_review_row(label, "done" if passed else "error")

                    if not passed:
                        # Add fixer row as running
                        fixer_label = f"Fixer {count}"
                        current_fixer_label = fixer_label
                        _upsert_review_row(fixer_label, "running", agent_num=6)
                    else:
                        # Reviewer passed — pipeline moves to write_output
                        wo["status"] = "running"
                        _refresh()

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    count = final_state.get("review_count", 1)
                    if current_fixer_label:
                        _finish_review_row(current_fixer_label, "done")
                        current_fixer_label = None
                    # Prime the next reviewer row with accurate start time
                    _upsert_review_row(f"Reviewer {count + 1}", "running", agent_num=5)

                # ── write_output ───────────────────────────────────────────────
                elif node_name == "write_output":
                    # Close any lingering running review rows (max-retries path)
                    _close_all_running_review_rows()
                    wo["status"] = "running"
                    _refresh()
                    _set_running("documenter")

                # ── all other fixed linear steps ───────────────────────────────
                else:
                    errored = node_output.get("status") in ("error", "halted")
                    _set_done(node_name, errored=errored)

                    # Advance next pending step — agents freely, documents only
                    # after write_output has fired (wo no longer "pending")
                    for key in [s["key"] for s in steps if s["section"] in ("agents", "documents")]:
                        obj = _step_by_key(key)
                        if obj and obj["status"] == "pending":
                            if obj["section"] == "documents" and wo["status"] == "pending":
                                break
                            _set_running(key)
                            break

                    # Generator done → activate the pre-seeded Reviewer 1
                    if node_name == "generator":
                        _upsert_review_row("Reviewer 1", "running", agent_num=5)

                    # STTM done → mark outputs as complete
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
    write_cost_summary(usage, cost)
    return final_state, {"cost": cost, "usage": usage}