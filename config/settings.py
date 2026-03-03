import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")
MAX_REVIEW_RETRIES = int(os.getenv("MAX_REVIEW_RETRIES", "3"))

INPUTS_DIR = BASE_DIR / "inputs"
OUTPUTS_DIR = BASE_DIR / "outputs" / "dbt_project"
LOGS_DIR = BASE_DIR / "logs"

SAS_SCRIPTS_DIR = INPUTS_DIR / "sas_scripts"
COLUMN_MAPPING_PATH = INPUTS_DIR / "column_mapping.json"
DBT_CONVENTIONS_PATH = INPUTS_DIR / "dbt_conventions.json"

INPUT_COST_PER_M = 1.25
OUTPUT_COST_PER_M = 10.0