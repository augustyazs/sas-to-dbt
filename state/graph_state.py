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
    sas_code_raw:      str
    sas_code_clean:    str
    ingestion_blocks:  list[str]
    column_mappings:   list[ColumnMapping]
    conventions:       DbtConventions
    analysis:          SASAnalysis
    resolved_mappings: ResolvedMappings
    migration_plan:    MigrationPlan
    dbt_project:       DbtProject
    review:            ReviewResult
    review_count:      int
    status:            str
    error:             str
    sas_documentation: str
    sttm_data:         dict