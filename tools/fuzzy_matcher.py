from difflib import get_close_matches


def fuzzy_match_table(table_name: str, available_tables: list[str], cutoff: float = 0.6) -> str | None:
    """Fuzzy match a table name against available options."""
    matches = get_close_matches(table_name.lower(), [t.lower() for t in available_tables], n=1, cutoff=cutoff)
    return matches[0] if matches else None


def fuzzy_match_column(column_name: str, available_columns: list[str], cutoff: float = 0.7) -> str | None:
    """Fuzzy match a column name against available options."""
    matches = get_close_matches(column_name.lower(), [c.lower() for c in available_columns], n=1, cutoff=cutoff)
    return matches[0] if matches else None