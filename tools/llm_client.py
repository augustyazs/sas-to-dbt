import json
import os
import time
from openai import OpenAI
from config.settings import (
    INPUT_COST_PER_M, OUTPUT_COST_PER_M,
    OPENAI_MODEL, OPENAI_MODEL_ANALYZER, OPENAI_MODEL_DOCUMENTER,
    OPENAI_MODEL_GENERATOR, OPENAI_MODEL_REVIEWER, OPENAI_MODEL_FIXER,
    OPENAI_MODEL_STTM, OPENAI_MODEL_ARCHITECT,
)

_client = None
_usage_log = []


def _get_client() -> OpenAI:
    """Lazy-init OpenAI client. Picks up key set at runtime."""
    global _client
    key = os.environ.get("OPENAI_API_KEY", "")
    if not key:
        raise RuntimeError("OPENAI_API_KEY not set. Enter it in the sidebar.")
    if _client is None or _client.api_key != key:
        _client = OpenAI(api_key=key)
    return _client


def reset_usage():
    """Clear usage log between runs."""
    global _usage_log
    _usage_log = []


def get_usage_log() -> list[dict]:
    return list(_usage_log)


def get_total_cost() -> dict:
    ti = sum(e["input_tokens"] for e in _usage_log)
    to = sum(e["output_tokens"] for e in _usage_log)
    tc = sum(e["cost_usd"] for e in _usage_log)
    tr = sum(e["response_time_seconds"] for e in _usage_log)
    return {
        "total_input_tokens": ti,
        "total_output_tokens": to,
        "total_cost_usd": round(tc, 6),
        "calls": len(_usage_log),
        "total_response_time_seconds": round(tr, 2),
    }


def _resolve_model(step_name: str) -> str:
    if step_name == "scout":
        return OPENAI_MODEL_SCOUT
    if step_name == "analyzer":
        return OPENAI_MODEL_ANALYZER
    elif step_name == "documenter_agent":
        return OPENAI_MODEL_DOCUMENTER
    elif step_name == "sttm_generator_agent":
        return OPENAI_MODEL_STTM
    elif step_name == "architect_plan":
        return OPENAI_MODEL_ARCHITECT
    elif step_name == "generator":
        return OPENAI_MODEL_GENERATOR
    elif step_name.startswith("reviewer_attempt"):
        return OPENAI_MODEL_REVIEWER
    elif step_name == "fixer":
        return OPENAI_MODEL_FIXER
    else:
        return OPENAI_MODEL


def _record_usage(step_name: str, model: str, inp: int, out: int, response_time: float):
    cost = (inp / 1_000_000 * INPUT_COST_PER_M) + (out / 1_000_000 * OUTPUT_COST_PER_M)
    _usage_log.append({
        "step": step_name,
        "model": model,
        "input_tokens": inp,
        "output_tokens": out,
        "cost_usd": round(cost, 6),
        "response_time_seconds": response_time,
    })


def call_llm(system_prompt: str, user_prompt: str, step_name: str = "") -> dict:
    """Send prompt to OpenAI, return parsed JSON."""
    client = _get_client()
    model = _resolve_model(step_name)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]
    try:
        t_start = time.perf_counter()
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={"type": "json_object"},
        )
    except Exception as e:
        print(f"\n  ❌ OpenAI API error [{step_name}] model={model}: {e}")
        raise
    response_time = round(time.perf_counter() - t_start, 2)
    usage = response.usage
    inp = usage.prompt_tokens     if usage else 0
    out = usage.completion_tokens if usage else 0
    _record_usage(step_name, model, inp, out, response_time)
    return json.loads(response.choices[0].message.content)


def call_llm_text(system_prompt: str, user_prompt: str, step_name: str = "") -> str:
    """Send prompt to OpenAI, return raw text (used by Documenter)."""
    client = _get_client()
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]
    t_start = time.perf_counter()
    response = client.chat.completions.create(
        model=OPENAI_MODEL_DOCUMENTER,
        messages=messages,
    )
    response_time = round(time.perf_counter() - t_start, 2)
    usage = response.usage
    inp = usage.prompt_tokens     if usage else 0
    out = usage.completion_tokens if usage else 0
    _record_usage(step_name, OPENAI_MODEL_DOCUMENTER, inp, out, response_time)
    return response.choices[0].message.content