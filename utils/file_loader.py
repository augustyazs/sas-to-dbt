import json
import csv
from pathlib import Path
from models.schemas import ColumnMapping, DbtConventions


def write_documentation(documentation: str, output_dir: Path) -> str:
    """Write pipeline documentation as documentation.md."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fp = output_dir / "documentation.md"
    fp.write_text(documentation, encoding="utf-8")
    return str(fp)


def load_source_script(path: Path) -> str:
    """
    Read source script. Handles Informatica XML specially —
    parses and converts to plain-text representation before returning.
    All other formats read as plain text with encoding fallback.
    """
    # Informatica XML — parse and convert to readable text
    if path.suffix.lower() == ".xml":
        from tools.informatica_parser import parse_informatica_xml, is_informatica_xml
        if is_informatica_xml(path):
            return parse_informatica_xml(path)
        # XML but not Informatica — read as plain text and let Scout/Analyzer decide
        return path.read_text(encoding="utf-8", errors="replace")

    # All other formats — plain text with encoding fallback
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
        try:
            text = path.read_text(encoding=enc)
            if text.strip():
                return text
        except (UnicodeDecodeError, ValueError):
            continue
    raise ValueError(f"Source script at {path} is empty or unreadable.")


def load_column_mapping(path: Path) -> list[ColumnMapping]:
    """Load column mapping from JSON or CSV."""
    if path.suffix == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"Column mapping {path.name} must be a JSON array, got {type(raw).__name__}")
        if not raw:
            return []
        if not isinstance(raw[0], dict):
            raise ValueError(
                f"Column mapping {path.name} contains {type(raw[0]).__name__} entries — "
                f"expected objects with source_schema, source_table, source_column, "
                f"target_schema, target_table, target_column keys."
            )
        return [ColumnMapping(**entry) for entry in raw]
    elif path.suffix == ".csv":
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [ColumnMapping(
                source_schema  = row.get("source_schema",  "").strip(),
                source_table   = row.get("source_table",   "").strip(),
                source_column  = row.get("source_column",  "").strip(),
                target_schema  = row.get("target_schema",  "").strip(),
                target_table   = row.get("target_table",   "").strip(),
                target_column  = row.get("target_column",  "").strip(),
            ) for row in reader]
    raise ValueError(f"Unsupported mapping format: {path.suffix}")


def load_conventions(path: Path) -> DbtConventions:
    """Load dbt conventions from JSON. Returns defaults if file missing or empty."""
    if path.exists():
        text = path.read_text(encoding="utf-8").strip()
        if text:
            return DbtConventions(**json.loads(text))
    return DbtConventions()