
# ── config/settings.py ───────────────────────────────────────────────────────

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# ── Per-agent model routing ───────────────────────────────────────────────────
OPENAI_MODEL            = os.getenv("OPENAI_MODEL",            "gpt-5")
OPENAI_MODEL_SCOUT      = os.getenv("OPENAI_MODEL_SCOUT",       "gpt-4.1")   # fast, runs first
OPENAI_MODEL_ANALYZER   = os.getenv("OPENAI_MODEL_ANALYZER",   "gpt-5")
OPENAI_MODEL_DOCUMENTER = os.getenv("OPENAI_MODEL_DOCUMENTER",  "gpt-4.1")
OPENAI_MODEL_STTM       = os.getenv("OPENAI_MODEL_STTM",        "gpt-4.1")
OPENAI_MODEL_ARCHITECT  = os.getenv("OPENAI_MODEL_ARCHITECT",   "gpt-5")
OPENAI_MODEL_GENERATOR  = os.getenv("OPENAI_MODEL_GENERATOR",   "gpt-5")
OPENAI_MODEL_REVIEWER   = os.getenv("OPENAI_MODEL_REVIEWER",    "gpt-5")
OPENAI_MODEL_FIXER      = os.getenv("OPENAI_MODEL_FIXER",       "gpt-5")

MAX_REVIEW_RETRIES = int(os.getenv("MAX_REVIEW_RETRIES", "3"))

# ── Paths ─────────────────────────────────────────────────────────────────────
INPUTS_DIR     = BASE_DIR / "inputs"
OUTPUTS_DIR    = BASE_DIR / "outputs" / "dbt_project"
DOC_OUTPUT_DIR = BASE_DIR / "outputs" / "documentation"
LOGS_DIR       = BASE_DIR / "logs"

SAS_SCRIPTS_DIR      = INPUTS_DIR / "sas_scripts"
COLUMN_MAPPING_PATH  = INPUTS_DIR / "column_mapping.json"
DBT_CONVENTIONS_PATH = INPUTS_DIR / "dbt_conventions.json"   # legacy — still read for dbt defaults

INPUT_COST_PER_M  = 1.25
OUTPUT_COST_PER_M = 10.0

# Supported target platforms
SUPPORTED_TARGETS = {"dbt", "pyspark", "scala", "sql"}


# ── tools/llm_client.py — add scout to _resolve_model ────────────────────────
# (only the _resolve_model function needs updating; rest of the file unchanged)

def _resolve_model_patch():
    """
    Patch for tools/llm_client.py _resolve_model function.
    Add this elif before the final else:

        elif step_name == "scout":
            return OPENAI_MODEL_SCOUT
    """
    pass  # This is a note — apply the change directly in tools/llm_client.py