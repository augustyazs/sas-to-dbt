from pydantic import BaseModel


class SourceTable(BaseModel):
    schema_name: str = ""
    table: str
    columns_used: list[str] = []
    access_method: str = "direct"


class IntermediateTable(BaseModel):
    table: str
    created_by: str = ""
    columns_produced: list[str] = []
    logic_summary: str = ""


class OutputTable(BaseModel):
    schema_name: str = ""
    table: str
    description: str = ""


class MacroInfo(BaseModel):
    name: str
    parameters: list[str] = []
    is_loop: bool = False
    loop_description: str = ""
    description: str = ""


class MacroVariable(BaseModel):
    name: str
    value: str = ""
    scope: str = "global"


class TransformationBlock(BaseModel):
    name: str
    type: str = ""
    input_tables: list[str] = []
    output_table: str = ""
    logic_summary: str = ""
    sql_hint: str = ""


class ReportingBlock(BaseModel):
    type: str
    description: str = ""
    note: str = "not convertible to dbt"


class SASAnalysis(BaseModel):
    source_tables: list[SourceTable] = []
    intermediate_tables: list[IntermediateTable] = []
    output_tables: list[OutputTable] = []
    macros: list[MacroInfo] = []
    macro_variables: list[MacroVariable] = []
    constructs: list[str] = []
    transformation_blocks: list[TransformationBlock] = []
    reporting_blocks: list[ReportingBlock] = []
    ingestion_flags: list[str] = []
    dependency_order: list[str] = []
    complexity_notes: list[str] = []
    logic_summary: str = ""


class ColumnMapping(BaseModel):
    source_schema: str
    source_table: str
    source_column: str
    target_schema: str
    target_table: str
    target_column: str


class ResolvedTable(BaseModel):
    original_schema: str = ""
    original_table: str
    resolved_schema: str
    resolved_table: str
    column_mappings: dict[str, str] = {}
    unresolved_columns: list[str] = []


class ResolvedMappings(BaseModel):
    tables: list[ResolvedTable] = []
    unresolved_tables: list[str] = []
    skipped_tables: list[str] = []
    warnings: list[str] = []


class DbtFile(BaseModel):
    path: str
    content: str


class DbtProject(BaseModel):
    dbt_project_yml: str = ""
    sources_yml: str = ""
    schema_yml: str = ""
    models: list[DbtFile] = []
    macros: list[DbtFile] = []
    not_converted: list[str] = []


class ReviewIssue(BaseModel):
    file: str = ""
    issue: str
    severity: str = "error"
    fix_suggestion: str = ""


class ReviewResult(BaseModel):
    is_valid: bool
    issues: list[ReviewIssue] = []
    summary: str = ""


class DbtConventions(BaseModel):
    materialization_staging: str = "view"
    materialization_intermediate: str = "table"
    materialization_marts: str = "table"
    prefix_staging: str = "stg_"
    prefix_intermediate: str = "int_"
    prefix_fact: str = "fct_"
    prefix_dimension: str = "dim_"
    target_dialect: str = "redshift"
    max_joins_per_model: int = 8
    notes: list[str] = []