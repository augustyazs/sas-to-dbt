import time
import streamlit as st
from models.schemas import ColumnMapping, DbtConventions
from graph.builder import build_graph
from tools.llm_client import get_usage_log, get_total_cost, reset_usage
from utils.logger import reset_logs

# Fixed linear steps in order (reviewer/fixer loop handled dynamically)
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
    "analyzer":    "Analyzer",
    "resolver":    "Resolver",
    "architect":   "Architect",
    "generator":   "Developer",
    "write_output":"Write Output",
    "documenter":  "Documenter",
    "sttm":        "STTM Generator",
}

# Steps that are LLM agents (vs infrastructure nodes)
AGENT_STEPS = {"analyzer", "resolver", "architect", "generator", "documenter", "sttm"}


def _fmt_time(seconds: float) -> str:
    """Format elapsed seconds as m:ss."""
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}:{s:02d}"


def run_pipeline(
    sas_code: str,
    mappings: list[ColumnMapping],
    conventions: DbtConventions,
    timeline_containers: dict,   # keyed by step name, values are st.empty()
    loop_area,                   # st.container() for reviewer/fixer dynamic rows
):
    """Run pipeline, updating the right-sidebar timeline as each node completes."""
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

    # Initialise all fixed steps to "not started"
    for step in STEP_ORDER:
        _render_timeline_row(
            timeline_containers[step], STEP_LABELS[step],
            status="not_started", elapsed=None,
            is_write_output=(step == "write_output"),
        )

    step_start_times: dict[str, float] = {}
    completed_steps:  list[str]        = []
    final_state:      dict             = {}

    # Track which step is currently "in progress" so we can set its start time
    def _mark_running(step: str):
        step_start_times[step] = time.perf_counter()
        _render_timeline_row(
            timeline_containers[step], STEP_LABELS[step],
            status="in_progress", elapsed=None,
            is_write_output=(step == "write_output"),
        )

    _mark_running("analyzer")

    try:
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "__end__":
                    continue

                final_state.update(node_output)
                elapsed = round(time.perf_counter() - step_start_times.get(node_name, time.perf_counter()), 1)

                # ── Reviewer ─────────────────────────────────────────────────
                if node_name == "reviewer":
                    count  = node_output.get("review_count", 1)
                    status = node_output.get("status", "")
                    ok     = status in ("complete", "complete_with_warnings")
                    n_err  = len([i for i in node_output.get("review", {}).issues
                                  if hasattr(node_output.get("review", object()), "issues")
                                  and i.severity == "error"]) if node_output.get("review") else 0

                    review = node_output.get("review")
                    n_err  = len([i for i in review.issues if i.severity == "error"]) if review else 0
                    n_warn = len([i for i in review.issues if i.severity == "warning"]) if review else 0

                    with loop_area:
                        c = st.empty()
                        _render_review_row(c, count, ok, n_err, n_warn, elapsed)

                    # If more passes needed, pre-create fixer slot
                    if not ok:
                        with loop_area:
                            fixer_slot = st.empty()
                            _render_timeline_row(
                                fixer_slot, f"Fixer (pass {count})",
                                status="in_progress", elapsed=None,
                            )
                            # store so fixer node can update it
                            final_state[f"_fixer_slot_{count}"] = fixer_slot
                        # Start fixer timer
                        step_start_times[f"fixer_{count}"] = time.perf_counter()
                    else:
                        # Mark write_output as next
                        _mark_running("write_output")

                # ── Fixer ─────────────────────────────────────────────────────
                elif node_name == "fixer":
                    count   = final_state.get("review_count", 1)
                    elapsed = round(time.perf_counter() - step_start_times.get(f"fixer_{count}", time.perf_counter()), 1)
                    slot    = final_state.get(f"_fixer_slot_{count}")
                    if slot:
                        _render_timeline_row(
                            slot, f"Fixer (pass {count})",
                            status="completed", elapsed=elapsed,
                        )
                    # Next: reviewer will run again — mark nothing yet, it will announce itself

                # ── Fixed linear steps ────────────────────────────────────────
                else:
                    if node_name in timeline_containers:
                        success = node_output.get("status") not in ("error", "halted")
                        _render_timeline_row(
                            timeline_containers[node_name],
                            STEP_LABELS[node_name],
                            status="completed" if success else "failed",
                            elapsed=elapsed,
                            is_write_output=(node_name == "write_output"),
                        )
                    completed_steps.append(node_name)

                    # Advance next fixed step to "in progress"
                    for s in STEP_ORDER:
                        if s not in completed_steps and s != "write_output":
                            _mark_running(s)
                            break
                        elif s == "write_output" and s not in completed_steps:
                            # write_output comes after reviewer loop — mark it when we get there
                            pass

    except Exception as e:
        st.error(f"Pipeline error: {str(e)}")
        return None, None

    cost  = get_total_cost()
    usage = get_usage_log()
    return final_state, {"cost": cost, "usage": usage}


# ── Timeline row renderers ────────────────────────────────────────────────────

_STATUS_CONFIG = {
    "not_started": ("⚪", "#888888", "Not Started"),
    "in_progress": ("🔵", "#1d6ae5", "In Progress"),
    "completed":   ("🟢", "#1e8c45", "Completed"),
    "failed":      ("🔴", "#c0392b", "Failed"),
}

def _render_timeline_row(
    container,
    label: str,
    status: str,
    elapsed: float | None,
    is_write_output: bool = False,
):
    icon, color, status_text = _STATUS_CONFIG.get(status, _STATUS_CONFIG["not_started"])
    time_str = f"  `{_fmt_time(elapsed)}`" if elapsed is not None else ""

    if is_write_output:
        container.markdown(
            f"<div style='border-top:1px solid #334155; margin:4px 0; padding:6px 0;'>"
            f"<span style='color:{color}; font-weight:600;'>{icon} {label}</span>"
            f"<span style='color:#888; font-size:12px;'> — {status_text}{time_str}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
    else:
        container.markdown(
            f"<div style='padding:3px 0 3px 8px; border-left:2px solid {color}; margin:2px 0;'>"
            f"<span style='font-weight:500;'>{icon} {label}</span>"
            f"<span style='color:#888; font-size:12px;'> — {status_text}{time_str}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )


def _render_review_row(container, attempt: int, passed: bool, n_err: int, n_warn: int, elapsed: float):
    icon  = "🟢" if passed else "🟡"
    color = "#1e8c45" if passed else "#e5a91d"
    label = f"Reviewer (pass {attempt})"
    detail = "Valid" if passed else f"{n_err} error(s), {n_warn} warning(s)"
    time_str = f"  `{_fmt_time(elapsed)}`"

    container.markdown(
        f"<div style='padding:3px 0 3px 8px; border-left:2px solid {color}; margin:2px 0;'>"
        f"<span style='font-weight:500;'>{icon} {label}</span>"
        f"<span style='color:#888; font-size:12px;'> — {detail}{time_str}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )