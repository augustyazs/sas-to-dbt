import json
from pathlib import Path
from datetime import datetime
from config.settings import LOGS_DIR

_current_run_logs = []


def reset_logs():
    """Clear current run log tracker."""
    global _current_run_logs
    _current_run_logs = []


def get_current_run_logs() -> list[Path]:
    """Return log file paths from the current run only."""
    return list(_current_run_logs)


def _ensure_dir():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def log_step(step_name: str, data, is_pydantic=True):
    """Dump step output to logs/ as formatted JSON."""
    _ensure_dir()
    ts = datetime.now().strftime("%H%M%S")
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
