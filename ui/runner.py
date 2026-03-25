import time
import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs

# Fixed linear steps in order (reviewer/fixer handled dynamically)
STEP_ORDER = [
    "analyzer",
    "resolver",
    "architect",
    "generator",
    "reviewer",   # placeholder — actual reviewer/fixer rows built dynamically
    "write_output",
    "documenter",
    "sttm",
]

STEP_LABELS = {
    "analyzer":    "Analyzer",
    "resolver":    "Resolver",
    "architect":   "Architect",
    "generator":   "Developer",
    "write_output":"Write Output",
    "documenter":  "Documenter",
    "sttm":        "STTM Generator",
}


def _fmt_time(seconds: float) -> str:
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}:{s:02d}"


def run_pipeline(
    sas_code: str,
    mappings: list[ColumnMapping],
    conventions: DbtConventions,
    status_container,
    step_containers: dict,   # kept for API compat but not used — we use session state
):
    """Run pipeline. Updates session_state.pipeline_steps as each node completes
    so timeline survives reruns. Returns (final_state, cost_data)."""
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

    # pipeline_steps: list of dicts, one per displayed row
    # shape: {label, status, elapsed, section, is_write_output}
    # Stored in session_state so timeline re-renders after reruns
    st.session_state.pipeline_steps = []
    step_start: dict[str, float] = {}

    def _push(label, status, elapsed=None, section="agents", is_write_output=False):
        st.session_state.pipeline_steps.append({
            "label":          label,
            "status":         status,
            "elapsed":        elapsed,
            "section":        section,
            "is_write_output":is_write_output,
        })

    def _update_last(status, elapsed=None):
        if st.session_state.pipeline_steps:
            st.session_state.pipeline_steps[-1]["status"]  = status
            st.session_state.pipeline_steps[-1]["elapsed"] = elapsed

    # Seed initial "in progress" for analyzer
    _push("Analyzer", "in_progress", section="agents")
    step_start["analyzer"] = time.perf_counter()

    final_state: dict = {}

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                final_state.update(node_output)
                elapsed = round(time.perf_counter() - step_start.get(node_name, time.perf_counter()), 1)

                # ── Reviewer ─────────────────────────────────────────────────
                if node_name == "reviewer":
                    count  = node_output.get("review_count", 1)
                    review = node_output.get("review")
                    n_err  = len([i for i in review.issues if i.severity == "error"]) if review else 0
                    n_warn = len([i for i in review.issues if i.severity == "warning"]) if review else 0
                    passed = node_output.get("status", "") in ("complete", "complete_with_warnings")

                    detail = "Valid" if passed else f"{n_err} error(s), {n_warn} warning(s)"
                    _push(
                        f"Reviewer (pass {count}) — {detail}",
                        "completed" if passed else "warning",
                        elapsed, section="review",
                    )
                    if not passed:
                        step_start[f"fixer_{count}"] = time.perf_counter()
                        _push(f"Fixer (pass {count})", "in_progress", section="review")
                    else:
                        step_start["write_output"] = time.perf_counter()
                        _push("Write Output", "in_progress", section="write_output", is_write_output=True)

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    count   = final_state.get("review_count", 1)
                    elapsed = round(time.perf_counter() - step_start.get(f"fixer_{count}", time.perf_counter()), 1)
                    _update_last("completed", elapsed)
                    # Next reviewer pass will be pushed when its event arrives
                    step_start[f"reviewer_{count+1}"] = time.perf_counter()
                    step_start[f"reviewer"] = time.perf_counter()

                # ── write_output ───────────────────────────────────────────────
                elif node_name == "write_output":
                    elapsed = round(time.perf_counter() - step_start.get("write_output", time.perf_counter()), 1)
                    _update_last("completed", elapsed)
                    step_start["documenter"] = time.perf_counter()
                    _push("Documenter", "in_progress", section="documents")

                # ── documenter ─────────────────────────────────────────────────
                elif node_name == "documenter":
                    elapsed = round(time.perf_counter() - step_start.get("documenter", time.perf_counter()), 1)
                    _update_last("completed", elapsed)
                    step_start["sttm"] = time.perf_counter()
                    _push("STTM Generator", "in_progress", section="documents")

                # ── sttm ───────────────────────────────────────────────────────
                elif node_name == "sttm":
                    elapsed = round(time.perf_counter() - step_start.get("sttm", time.perf_counter()), 1)
                    _update_last("completed", elapsed)

                # ── other linear steps (analyzer, resolver, architect, generator)
                else:
                    elapsed = round(time.perf_counter() - step_start.get(node_name, time.perf_counter()), 1)
                    success = node_output.get("status") not in ("error", "halted")
                    _update_last("completed" if success else "failed", elapsed)

                    # Push next step as in_progress
                    _next = {
                        "analyzer":  ("resolver",  "Resolver",  "agents"),
                        "resolver":  ("architect", "Architect", "agents"),
                        "architect": ("generator", "Developer", "agents"),
                        "generator": None,   # reviewer comes next but is dynamic
                    }.get(node_name)
                    if _next:
                        step_start[_next[0]] = time.perf_counter()
                        _push(_next[1], "in_progress", section=_next[2])
                    elif node_name == "generator":
                        # First reviewer pass is coming
                        step_start["reviewer"] = time.perf_counter()
                        _push("Reviewer (pass 1)", "in_progress", section="review")

    except Exception as e:
        status_container.error(f"Pipeline error: {str(e)}")
        return None, None

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}