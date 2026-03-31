import json
import time
from pathlib import Path

_current_run_logs: list[Path] = []
_run_start_time: float = 0.0
_logs_dir: Path = Path("logs")   # overridden by reset_logs(dir)


def reset_logs(logs_dir: Path | None = None):
    """Clear current run log tracker, set log directory, start run timer."""
    global _current_run_logs, _run_start_time, _logs_dir
    _current_run_logs = []
    _run_start_time   = time.perf_counter()
    if logs_dir:
        _logs_dir = logs_dir
    _logs_dir.mkdir(parents=True, exist_ok=True)


def get_current_run_logs() -> list[Path]:
    return list(_current_run_logs)


def log_step(step_name: str, data, is_pydantic: bool = True):
    """Dump step output to <logs_dir>/<step_name>.json."""
    _logs_dir.mkdir(parents=True, exist_ok=True)

    if is_pydantic:
        content = data.model_dump_json(indent=2)
    elif isinstance(data, str):
        content = data
    else:
        content = json.dumps(data, indent=2, default=str)

    path = _logs_dir / f"{step_name}.json"
    path.write_text(content, encoding="utf-8")

    if path not in _current_run_logs:
        _current_run_logs.append(path)

    print(f"  Log: {path}")


def write_cost_summary(usage_log: list[dict], total: dict) -> Path:
    """Write structured cost_summary.json to the current log dir."""
    _logs_dir.mkdir(parents=True, exist_ok=True)
    summary: dict = {}

    for entry in usage_log:
        step = entry.get("step") or "unknown"
        summary[step] = {
            "input_tokens":          entry["input_tokens"],
            "output_tokens":         entry["output_tokens"],
            "cost_usd":              entry["cost_usd"],
            "response_time_seconds": entry["response_time_seconds"],
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

    path = _logs_dir / "cost_summary.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if path not in _current_run_logs:
        _current_run_logs.append(path)

    print(f"  Cost summary: {path}")
    return path