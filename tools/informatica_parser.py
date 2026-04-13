"""
Informatica XML mapping parser.

Converts a PowerMart/PowerCenter mapping XML into a compact plain-text
representation that Scout and Analyzer can process like any other source script.

Extracts:
  - Source table definitions and columns
  - Target table definitions and columns
  - All transformation types with their logic (SQ SQL, EXP expressions,
    JNR join conditions, AGG group-by/aggregates, RTR filter groups,
    LKP lookup conditions)
  - Connector flow (data lineage between transformations)
"""

import xml.etree.ElementTree as ET
from pathlib import Path


def parse_informatica_xml(path: Path) -> str:
    """
    Parse an Informatica mapping XML file and return a plain-text
    representation suitable for LLM analysis.

    Returns a string — drop-in replacement for raw source code text.
    Raises ValueError if the file is not a valid Informatica mapping.
    """
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML in {path.name}: {e}")

    # Find the MAPPING element — may be nested under FOLDER/REPOSITORY
    mapping = root.find(".//MAPPING")
    if mapping is None:
        raise ValueError(f"{path.name} does not contain a MAPPING element — not an Informatica mapping file.")

    mapping_name = mapping.get("NAME", "unknown")
    mapping_desc = mapping.get("DESCRIPTION", "")

    sections: list[str] = []

    sections.append(f"INFORMATICA MAPPING: {mapping_name}")
    if mapping_desc:
        sections.append(f"Description: {mapping_desc}")
    sections.append("")

    # ── Sources ───────────────────────────────────────────────────────────────
    sources = mapping.findall("SOURCE")
    if sources:
        sections.append("=" * 60)
        sections.append("SOURCE TABLES")
        sections.append("=" * 60)
        for src in sources:
            db     = src.get("DBDNAME", "")
            owner  = src.get("OWNERNAME", "")
            name   = src.get("NAME", "")
            desc   = src.get("DESCRIPTION", "")
            schema = f"{owner}." if owner else ""
            db_str = f" (database: {db})" if db else ""
            sections.append(f"\n  [{name}]{db_str}")
            if desc:
                sections.append(f"  Description: {desc}")
            cols = src.findall("SOURCEFIELD")
            if cols:
                sections.append("  Columns:")
                for c in cols:
                    dtype   = c.get("DATATYPE", "")
                    width   = c.get("FIELDWIDTH", "")
                    prec    = c.get("FIELDPRECISION", "")
                    null    = c.get("NULLABLE", "")
                    type_str = f"{dtype}({width})" if width and not prec else \
                               f"{dtype}({width},{prec})" if width and prec else dtype
                    sections.append(f"    - {c.get('NAME','')}  {type_str}  {null}")

    # ── Targets ───────────────────────────────────────────────────────────────
    targets = mapping.findall("TARGET")
    if targets:
        sections.append("\n" + "=" * 60)
        sections.append("TARGET TABLES")
        sections.append("=" * 60)
        for tgt in targets:
            owner = tgt.get("OWNERNAME", "")
            name  = tgt.get("NAME", "")
            desc  = tgt.get("DESCRIPTION", "")
            sections.append(f"\n  [{name}]")
            if desc:
                sections.append(f"  Description: {desc}")
            cols = tgt.findall("TARGETFIELD")
            if cols:
                sections.append("  Columns:")
                for c in cols:
                    dtype   = c.get("DATATYPE", "")
                    width   = c.get("FIELDWIDTH", "")
                    prec    = c.get("FIELDPRECISION", "")
                    null    = c.get("NULLABLE", "")
                    type_str = f"{dtype}({width})" if width and not prec else \
                               f"{dtype}({width},{prec})" if width and prec else dtype
                    sections.append(f"    - {c.get('NAME','')}  {type_str}  {null}")

    # ── Transformations ───────────────────────────────────────────────────────
    transformations = mapping.findall("TRANSFORMATION")
    if transformations:
        sections.append("\n" + "=" * 60)
        sections.append("TRANSFORMATIONS")
        sections.append("=" * 60)

        for t in transformations:
            t_name = t.get("NAME", "")
            t_type = t.get("TYPE", "")
            t_desc = t.get("DESCRIPTION", "")
            sections.append(f"\n  [{t_name}]  Type: {t_type}")
            if t_desc:
                sections.append(f"  Description: {t_desc}")

            fields      = t.findall("TRANSFORMFIELD")
            table_attrs = {
                a.get("NAME"): a.get("VALUE")
                for a in t.findall("TABLEATTRIBUTE")
            }

            # Source Qualifier — show SQL override if present
            if t_type == "Source Qualifier":
                sql = table_attrs.get("Sql Query", "").strip()
                if sql:
                    sections.append(f"  SQL Override:")
                    for line in sql.splitlines():
                        sections.append(f"    {line.strip()}")
                else:
                    sections.append("  SQL Override: (none — reads all columns from source)")
                _append_port_list(sections, fields)

            # Expression — show each output expression
            elif t_type == "Expression":
                sections.append("  Expressions:")
                for f in fields:
                    expr = f.get("EXPRESSION", "").strip()
                    port = f.get("PORTTYPE", "")
                    if expr and "OUTPUT" in port:
                        sections.append(f"    {f.get('NAME','')} = {expr}")
                _append_port_list(sections, fields, only_no_expr=True)

            # Joiner — show join type and condition
            elif t_type == "Joiner":
                join_type  = table_attrs.get("Join Type", "Inner")
                join_cond  = table_attrs.get("Join Condition", "")
                master_src = table_attrs.get("Master Source", "")
                sections.append(f"  Join Type  : {join_type}")
                sections.append(f"  Master     : {master_src}")
                sections.append(f"  Condition  : {join_cond}")
                _append_port_list(sections, fields)

            # Aggregator — show group-by fields and aggregate expressions
            elif t_type == "Aggregator":
                sections.append("  Group By:")
                for f in fields:
                    if f.get("GROUP", "").upper() == "YES":
                        sections.append(f"    - {f.get('NAME','')}")
                sections.append("  Aggregates:")
                for f in fields:
                    expr = f.get("EXPRESSION", "").strip()
                    port = f.get("PORTTYPE", "")
                    if expr and "OUTPUT" in port and f.get("GROUP", "").upper() != "YES":
                        sections.append(f"    {f.get('NAME','')} = {expr}")

            # Router — show each filter group
            elif t_type == "Router":
                sections.append("  Filter Groups:")
                groups = t.findall("TABLEATTRIBUTE")
                for g in groups:
                    gname = g.get("NAME", "")
                    gval  = g.get("VALUE", "")
                    if gname.startswith("Group Filter Condition"):
                        grp_id = gname.replace("Group Filter Condition", "").strip()
                        sections.append(f"    Group {grp_id}: WHERE {gval}")
                _append_port_list(sections, fields)

            # Lookup — show lookup condition and return ports
            elif t_type == "Lookup Procedure" or t_type == "Lookup":
                lkp_cond  = table_attrs.get("Lookup Condition", "")
                lkp_table = table_attrs.get("Lookup table name", "")
                if lkp_table:
                    sections.append(f"  Lookup Table: {lkp_table}")
                if lkp_cond:
                    sections.append(f"  Condition   : {lkp_cond}")
                _append_port_list(sections, fields)

            # Filter
            elif t_type == "Filter":
                filt = table_attrs.get("Filter Condition", "")
                sections.append(f"  Filter Condition: {filt}")
                _append_port_list(sections, fields)

            # Sorter
            elif t_type == "Sorter":
                sections.append("  Sort Keys:")
                for f in fields:
                    direction = f.get("SORTDIRECTION", "")
                    if direction:
                        sections.append(f"    - {f.get('NAME','')} {direction}")

            # Any other type — just show ports
            else:
                _append_port_list(sections, fields)

    # ── Connector flow ────────────────────────────────────────────────────────
    connectors = mapping.findall("CONNECTOR")
    if connectors:
        sections.append("\n" + "=" * 60)
        sections.append("DATA FLOW (connectors)")
        sections.append("=" * 60)
        # Group by target instance for readability
        flow: dict[str, list[str]] = {}
        for c in connectors:
            from_inst  = c.get("FROMINSTANCE", "")
            from_field = c.get("FROMFIELD", "")
            to_inst    = c.get("TOINSTANCE", "")
            to_field   = c.get("TOFIELD", "")
            key = f"{from_inst} → {to_inst}"
            flow.setdefault(key, []).append(f"{from_field} → {to_field}")
        for edge, fields_list in flow.items():
            sections.append(f"\n  {edge}")
            for f in fields_list:
                sections.append(f"    {f}")

    return "\n".join(sections)


def _append_port_list(
    sections: list[str],
    fields: list,
    only_no_expr: bool = False,
) -> None:
    """Append INPUT/OUTPUT port summary to sections."""
    inputs  = [f.get("NAME","") for f in fields if "INPUT"  in f.get("PORTTYPE","") and not f.get("EXPRESSION","")]
    outputs = [f.get("NAME","") for f in fields if "OUTPUT" in f.get("PORTTYPE","") and not f.get("EXPRESSION","")]
    if inputs:
        sections.append(f"  Input ports : {', '.join(inputs)}")
    if outputs:
        sections.append(f"  Output ports: {', '.join(outputs)}")


def is_informatica_xml(path: Path) -> bool:
    """
    Quick check — does this XML file look like an Informatica mapping?
    Checks for POWERMART root or MAPPING element near the top.
    """
    if path.suffix.lower() != ".xml":
        return False
    try:
        # Only read first 2KB to check root tag
        with open(path, encoding="utf-8", errors="ignore") as f:
            head = f.read(2048)
        return "POWERMART" in head or "MAPPING" in head
    except Exception:
        return False