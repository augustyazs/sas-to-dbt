import re


INGESTION_PATTERNS = [
    (r'^\s*x\s+["\'].*?["\'];\s*$', "shell command"),
    (r'^\s*x\s+\w+.*?;\s*$', "shell command"),
    (r'(?si)DATA\s+\w+;\s*INFILE\s+.*?RUN;', "INFILE data step"),
    (r'(?si)execute\s*\(\s*(?:INSERT\s+INTO|TRUNCATE|DROP\s+TABLE|CREATE\s+TABLE\s+\w+\s+AS\s*\n?\s*SELECT\s+\*\s+FROM\s*\n?\s*EXTERNAL).*?\)\s*by\s+\w+;', "passthrough DDL/load"),
    (r'(?si)PROC\s+APPEND\s+.*?RUN;', "PROC APPEND"),
    (r'(?si)PROC\s+DELETE\s+.*?RUN;', "PROC DELETE"),
    (r'(?si)PROC\s+DATASETS\s+.*?(?:RUN|QUIT);', "PROC DATASETS"),
    (r'(?si)PROC\s+EXPORT\s+.*?RUN;', "PROC EXPORT"),
]

REPORTING_PATTERNS = [
    (r'(?si)ODS\s+EXCEL\s+.*?;', "ODS EXCEL"),
    (r'(?si)ODS\s+_ALL_\s+.*?;', "ODS close"),
    (r'(?si)ODS\s+LISTING\s+.*?;', "ODS listing"),
    (r'(?si)ODS\s+HTML\s+.*?;', "ODS HTML"),
    (r'(?si)PROC\s+REPORT\s+.*?RUN;', "PROC REPORT"),
    (r'(?si)PROC\s+ODSTEXT\s*;.*?RUN;', "PROC ODSTEXT"),
    (r'(?si)PROC\s+TEMPLATE\s*;.*?RUN;', "PROC TEMPLATE"),
    (r'(?si)FILENAME\s+\w+\s+email.*?;', "email setup"),
    (r'(?si)DATA\s+_NULL_\s*;\s*file\s+mymail.*?RUN;', "email send"),
    (r'(?si)TITLE\d*\s+.*?;', "TITLE statement"),
    (r'(?si)FOOTNOTE\d*\s+.*?;', "FOOTNOTE statement"),
]


def preprocess_sas(raw_code: str) -> tuple[str, list[str]]:
    """Strip ingestion and reporting blocks from SAS code. Returns (clean_code, flagged_blocks)."""
    flagged = []
    clean = raw_code

    for pattern, label in INGESTION_PATTERNS:
        matches = re.findall(pattern, clean, re.MULTILINE)
        for m in matches:
            flagged.append(f"[INGESTION - {label}]: {m[:120].strip()}...")
        clean = re.sub(pattern, "", clean, flags=re.MULTILINE)

    for pattern, label in REPORTING_PATTERNS:
        matches = re.findall(pattern, clean, re.MULTILINE)
        for m in matches:
            flagged.append(f"[REPORTING - {label}]: {m[:120].strip()}...")
        clean = re.sub(pattern, "", clean, flags=re.MULTILINE)

    clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)
    clean = re.sub(r'\n{3,}', '\n\n', clean)

    return clean.strip(), flagged