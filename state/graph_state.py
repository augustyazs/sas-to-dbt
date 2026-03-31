from typing import TypedDict
from models.schemas import (
    SASAnalysis,
    ColumnMapping,
    ResolvedMappings,
    DbtProject,
    DbtConventions,
    ReviewResult,
    MigrationPlan,
)


class GraphState(TypedDict, total=False):
    # ── Raw inputs ────────────────────────────────────────────────────────────
    sas_code_raw:        str
    target_platform:     str          # "dbt" | "pyspark" | "scala" | "sql"

    # ── Scout outputs ─────────────────────────────────────────────────────────
    detected_language:   str          # e.g. "SAS", "PySpark", "PL/SQL"
    input_conventions:   dict         # briefing for Analyzer
    output_conventions:  dict         # briefing for Generator / Reviewer / Fixer

    # ── Preprocessor outputs ──────────────────────────────────────────────────
    sas_code_clean:      str          # clean source code (name kept for compat)
    ingestion_blocks:    list[str]    # stripped blocks log

    # ── Pipeline inputs ───────────────────────────────────────────────────────
    column_mappings:     list[ColumnMapping]
    conventions:         DbtConventions   # kept for dbt path; ignored for others

    # ── Agent outputs ─────────────────────────────────────────────────────────
    analysis:            SASAnalysis
    resolved_mappings:   ResolvedMappings
    migration_plan:      MigrationPlan
    dbt_project:         DbtProject
    review:              ReviewResult
    review_count:        int
    status:              str
    error:               str

    # ── Runtime paths (set by main.py, per script) ───────────────────────────
    outputs_dir:         str
    doc_output_dir:      str
    logs_dir:            str

    # ── Documentation outputs ─────────────────────────────────────────────────
    sas_documentation:   str
    sttm_data:           dict