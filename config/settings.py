import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# ── Per-agent model routing ───────────────────────────────────────────────────
OPENAI_MODEL            = os.getenv("OPENAI_MODEL",            "gpt-5")
OPENAI_MODEL_SCOUT      = os.getenv("OPENAI_MODEL_SCOUT",      "gpt-5")
OPENAI_MODEL_ANALYZER   = os.getenv("OPENAI_MODEL_ANALYZER",   "gpt-5")
OPENAI_MODEL_DOCUMENTER = os.getenv("OPENAI_MODEL_DOCUMENTER",  "gpt-4.1")
OPENAI_MODEL_STTM       = os.getenv("OPENAI_MODEL_STTM",        "gpt-4.1")
OPENAI_MODEL_ARCHITECT  = os.getenv("OPENAI_MODEL_ARCHITECT",   "gpt-5")
OPENAI_MODEL_GENERATOR  = os.getenv("OPENAI_MODEL_GENERATOR",   "gpt-5")
OPENAI_MODEL_REVIEWER   = os.getenv("OPENAI_MODEL_REVIEWER",    "gpt-5")
OPENAI_MODEL_FIXER      = os.getenv("OPENAI_MODEL_FIXER",       "gpt-5")

MAX_REVIEW_RETRIES = int(os.getenv("MAX_REVIEW_RETRIES", "3"))

# ── Paths ─────────────────────────────────────────────────────────────────────
INPUT_SCRIPTS_DIR  = BASE_DIR / "inputs" / "scripts"
COLUMN_MAPPING_DIR = BASE_DIR / "inputs" / "column_mappings"

# These are set dynamically at runtime once the script name is known
# Use get_output_dirs(script_stem) to resolve them
def get_output_dirs(script_stem: str) -> dict:
    return {
        "outputs":       BASE_DIR / "outputs" / "project"       / script_stem,
        "docs":          BASE_DIR / "outputs" / "documentation"  / script_stem,
        "logs":          BASE_DIR / "logs"                       / script_stem,
    }

INPUT_COST_PER_M  = 1.25
OUTPUT_COST_PER_M = 10.0

# Supported target platforms
SUPPORTED_TARGETS = {"dbt", "pyspark"}