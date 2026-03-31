import re
from pathlib import Path


EXTENSION_MAP = {
    ".sas":   "SAS",
    ".py":    "PySpark",   # refined to Python vs PySpark via signals below
    ".r":     "R",
    ".scala": "Scala",
    ".pls":   "PL/SQL",   # Oracle package spec/body
    ".pkb":   "PL/SQL",   # Oracle package body
    ".pks":   "PL/SQL",   # Oracle package spec
    ".prc":   "PL/SQL",   # Oracle stored procedure
    ".fnc":   "PL/SQL",   # Oracle function
    ".trg":   "PL/SQL",   # Oracle trigger
    # .sql intentionally excluded — too ambiguous (ANSI SQL vs PL/SQL vs T-SQL)
}

# Signal patterns per language — need 2+ hits to confirm
SIGNAL_MAP = {
    "SAS":     [r'\bdata\s+\w+\s*;', r'\bproc\s+sql\b', r'%macro\b',
                r'\blibname\b', r'\brun\s*;', r'\bquit\s*;'],
    # "PySpark": [r'\bSparkSession\b', r'spark\.read', r'\.withColumnRenamed',
    #             r'\bpyspark\b', r'from\s+pyspark', r'\.toDF\b'],
    "Python":  [r'\bpd\.read', r'\bpandas\b', r'import\s+pandas',
                r'sqlalchemy', r'def\s+\w+\s*\('],
    "Scala":   [r'\bval\s+\w+\s*:', r'SparkSession\.builder',
                r'\bcase\s+class\b', r'\bDataFrame\b', r'import\s+org\.apache'],
    "R":       [r'<-\s', r'\blibrary\s*\(', r'dplyr::', r'%>%',
                r'\btibble\b', r'\bggplot\b'],
    "PL/SQL":  [r'\bBEGIN\b', r'\bEXCEPTION\b', r'\bEND\s*;',
                r'\bCURSOR\b', r'\bBULK\s+COLLECT\b'],
    "SQL":     [r'\bSELECT\b', r'\bCREATE\s+TABLE\b',
                r'\bWITH\b.*\bAS\b', r'\bINSERT\s+INTO\b'],
}


def detect_language(path: Path, content: str) -> str | None:
    """
    Returns detected language string, or None if genuinely ambiguous.

    Order:
      1. File extension  — fast, reliable for non-.txt files
      2. Keyword signals — scored match for .txt or extension conflicts
      3. None            — hand off to LLM in Scout
    """
    ext = path.suffix.lower() if path else ""

    # Step 1: extension match (skip .txt — needs content inspection)
    if ext in EXTENSION_MAP and ext != ".txt":
        base_lang = EXTENSION_MAP[ext]
        # For .py, disambiguate Python vs PySpark via signals
        if ext == ".py":
            spark_hits = sum(1 for p in SIGNAL_MAP["PySpark"]
                             if re.search(p, content[:8000], re.IGNORECASE))
            return "PySpark" if spark_hits >= 2 else "Python"
        return base_lang

    # Step 2: score all languages by signal hits
    sample = content[:8000]
    scores: dict[str, int] = {}
    for lang, patterns in SIGNAL_MAP.items():
        hits = sum(1 for p in patterns if re.search(p, sample, re.IGNORECASE))
        if hits >= 2:
            scores[lang] = hits

    if scores:
        return max(scores, key=lambda k: scores[k])

    # Step 3: single hit — weaker confidence, still return best guess
    weak: dict[str, int] = {}
    for lang, patterns in SIGNAL_MAP.items():
        hits = sum(1 for p in patterns if re.search(p, sample, re.IGNORECASE))
        if hits == 1:
            weak[lang] = hits

    if weak:
        best = max(weak, key=lambda k: weak[k])
        print(f"  [detector] Weak signal — best guess: {best} (1 hit). "
              f"Scout LLM will confirm.")
        return best

    return None  # genuinely ambiguous — Scout LLM takes over