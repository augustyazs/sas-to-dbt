import json
import os
from openai import OpenAI
from config.settings import OPENAI_API_KEY, OPENAI_MODEL, INPUT_COST_PER_M, OUTPUT_COST_PER_M

api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY not found.")

client = OpenAI(api_key=api_key)

_usage_log = []


def get_usage_log() -> list[dict]:
    return list(_usage_log)


def get_total_cost() -> dict:
    ti = sum(e["input_tokens"] for e in _usage_log)
    to = sum(e["output_tokens"] for e in _usage_log)
    tc = sum(e["cost_usd"] for e in _usage_log)
    return {"total_input_tokens": ti, "total_output_tokens": to, "total_cost_usd": round(tc, 6), "calls": len(_usage_log)}


def call_llm(system_prompt: str, user_prompt: str, step_name: str = "") -> dict:
    """Send prompt to OpenAI, return parsed JSON."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    kwargs = dict(model=OPENAI_MODEL, messages=messages, response_format={"type": "json_object"})

    response = client.chat.completions.create(**kwargs)

    usage = response.usage
    inp = usage.prompt_tokens if usage else 0
    out = usage.completion_tokens if usage else 0
    cost = (inp / 1_000_000 * INPUT_COST_PER_M) + (out / 1_000_000 * OUTPUT_COST_PER_M)

    _usage_log.append({"step": step_name, "model": OPENAI_MODEL, "input_tokens": inp, "output_tokens": out, "cost_usd": round(cost, 6)})

    content = response.choices[0].message.content
    return json.loads(content)