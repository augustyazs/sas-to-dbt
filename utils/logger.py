import json
import time
from pathlib import Path
from datetime import datetime
from config.settings import LOGS_DIR

_current_run_logs: list[Path] = []
_run_start_time: float = 0.0


def reset_logs():
    global _current_run_logs, _run_start_time
    _current_run_logs = []
    _run_start_time   = time.perf_counter()


def get_current_run_logs() -> list[Path]:
    return list(_current_run_logs)


def _ensure_dir():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def log_step(step_name: str, data, is_pydantic=True):
    """Dump step output to logs/ as formatted JSON."""
    _ensure_dir()
    ts       = datetime.now().strftime("%H%M%S")
    filename = f"{step_name}_{ts}.json"

    if is_pydantic:
        content = data.model_dump_json(indent=2)
    elif isinstance(data, str):
        content = data
    else:
        content = json.dumps(data, indent=2, default=str)

    path = LOGS_DIR / filename
    path.write_text(content, encoding="utf-8")
    _current_run_logs.append(path)
    print(f"  📝 Log: {path}")


def write_cost_summary(usage_log: list[dict], total: dict) -> Path:
    """
    Write structured cost_summary.json alongside per-step logs.
    Format mirrors the example:
    { "<step>": { input_tokens, output_tokens, cost_usd, response_time_seconds }, ...,
      "all_agents": { ... }, "total_run_duration_seconds": ... }
    """
    _ensure_dir()
    summary: dict = {}

    for entry in usage_log:
        step = entry.get("step") or "unknown"
        summary[step] = {
            "input_tokens":          entry["input_tokens"],
            "output_tokens":         entry["output_tokens"],
            "cost_usd":              entry["cost_usd"],
            "response_time_seconds": entry["response_time_seconds"],
        }

    # resolver has no LLM calls — always inject a zero entry
    if "resolver_agent" not in summary:
        summary["resolver_agent"] = {
            "input_tokens":          0,
            "output_tokens":         0,
            "cost_usd":              0.0,
            "response_time_seconds": 0.0,
        }

    summary["all_agents"] = {
        "input_tokens":          total["total_input_tokens"],
        "output_tokens":         total["total_output_tokens"],
        "cost_usd":              total["total_cost_usd"],
        "total_api_calls":       total["calls"],
        "response_time_seconds": total["total_response_time_seconds"],
    }

    run_duration = round(time.perf_counter() - _run_start_time, 2) if _run_start_time else 0.0
    summary["total_run_duration_seconds"] = run_duration

    ts   = datetime.now().strftime("%H%M%S")
    path = LOGS_DIR / f"cost_summary_{ts}.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _current_run_logs.append(path)
    print(f"  📝 Cost summary: {path}")
    return path