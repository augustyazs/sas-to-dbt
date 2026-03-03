from state.graph_state import GraphState
from models.schemas import ResolvedTable, ResolvedMappings
from tools.mapping_lookup import build_lookup, exact_lookup_table, exact_lookup_column
from tools.fuzzy_matcher import fuzzy_match_table, fuzzy_match_column
from utils.logger import log_step

SKIP_SCHEMAS = {"work", ""}
SKIP_PREFIXES = {"work.", "_null_"}


def _should_skip(schema: str, table: str) -> bool:
    """Skip WORK.* tables and internal SAS temp tables."""
    if schema.lower() in SKIP_SCHEMAS and table.lower() not in _get_source_candidates(schema):
        return True
    if any(table.lower().startswith(p) for p in SKIP_PREFIXES):
        return True
    return False


def _get_source_candidates(schema: str) -> set:
    return set()


def resolver_node(state: GraphState) -> dict:
    """Resolve on-prem table/column refs to cloud names using mapping file."""
    print("\n[RESOLVER] Resolving schema mappings...")

    analysis = state["analysis"]
    mappings = state["column_mappings"]
    lookup = build_lookup(mappings)

    available_tables = list({(m.source_schema.lower(), m.source_table.lower()) for m in mappings})
    available_table_names = [t[1] for t in available_tables]

    resolved_tables = []
    unresolved_tables = []
    skipped_tables = []
    warnings = []

    all_tables = analysis.source_tables + [
        type("Obj", (), {"schema_name": t.schema_name, "table": t.table, "columns_used": t.columns_produced})()
        for t in analysis.intermediate_tables if hasattr(t, "schema_name")
    ]

    for table_ref in analysis.source_tables:
        schema = getattr(table_ref, "schema_name", "")
        table = table_ref.table

        if schema.lower() in {"work", ""} and table.lower() in {
            it.table.lower() for it in analysis.intermediate_tables
        }:
            skipped_tables.append(f"{schema}.{table}" if schema else table)
            continue

        key = (schema.lower(), table.lower())
        target_info = lookup.get(key)

        if not target_info:
            fuzzy = fuzzy_match_table(table, available_table_names)
            if fuzzy:
                matched_key = next((k for k in available_tables if k[1] == fuzzy), None)
                if matched_key:
                    target_info = lookup.get(matched_key)
                    warnings.append(f"Fuzzy matched table '{table}' -> '{fuzzy}'")

        if not target_info:
            unresolved_tables.append(f"{schema}.{table}" if schema else table)
            continue

        col_mappings = {}
        unresolved_cols = []

        for col in table_ref.columns_used:
            resolved_col = exact_lookup_column(col, target_info)
            if resolved_col:
                col_mappings[col] = resolved_col
            else:
                fuzzy_col = fuzzy_match_column(col, list(target_info["columns"].keys()))
                if fuzzy_col:
                    col_mappings[col] = target_info["columns"][fuzzy_col]
                    warnings.append(f"Fuzzy matched column '{col}' -> '{fuzzy_col}' in {table}")
                else:
                    unresolved_cols.append(col)

        resolved_tables.append(ResolvedTable(
            original_schema=schema,
            original_table=table,
            resolved_schema=target_info["target_schema"],
            resolved_table=target_info["target_table"],
            column_mappings=col_mappings,
            unresolved_columns=unresolved_cols,
        ))

    resolved = ResolvedMappings(
        tables=resolved_tables,
        unresolved_tables=unresolved_tables,
        skipped_tables=skipped_tables,
        warnings=warnings,
    )

    print(f"  Resolved: {len(resolved_tables)} tables")
    print(f"  Skipped (WORK/internal): {len(skipped_tables)}")
    print(f"  Unresolved: {unresolved_tables}")
    if warnings:
        for w in warnings:
            print(f"  ⚠ {w}")

    log_step("resolver_output", resolved)

    status = "resolved"
    if unresolved_tables:
        critical = [t for t in unresolved_tables if not any(
            t.lower().startswith(s) for s in ["work.", ""]
        )]
        if critical:
            status = "unresolved_critical"

    return {"resolved_mappings": resolved, "status": status}