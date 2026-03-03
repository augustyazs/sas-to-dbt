from models.schemas import ColumnMapping


def build_lookup(mappings: list[ColumnMapping]) -> dict:
    """Build nested lookup: (src_schema, src_table) -> {target_info + columns}."""
    lookup = {}
    for m in mappings:
        key = (m.source_schema.lower(), m.source_table.lower())
        if key not in lookup:
            lookup[key] = {
                "target_schema": m.target_schema,
                "target_table": m.target_table,
                "columns": {},
            }
        lookup[key]["columns"][m.source_column.lower()] = m.target_column
    return lookup


def exact_lookup_table(table: str, schema: str, lookup: dict) -> dict | None:
    """Exact match a table in the lookup. Returns target info or None."""
    key = (schema.lower(), table.lower())
    return lookup.get(key)


def exact_lookup_column(column: str, table_info: dict) -> str | None:
    """Exact match a column within a resolved table. Returns target column or None."""
    return table_info["columns"].get(column.lower())