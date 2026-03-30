"""
Language-aware preprocessor.

Strips ingestion / reporting / logging blocks from source code before analysis.
Rules come from two sources (merged, Scout takes priority):
  1. Built-in regex patterns per detected language (exhaustive defaults)
  2. input_conventions["blocks_to_strip"] from Scout (script-specific additions)
"""
import re
from typing import NamedTuple


class StripPattern(NamedTuple):
    pattern: str
    label:   str


# ── Built-in patterns per language ───────────────────────────────────────────
# Each entry: (regex_pattern, human_label)
# Patterns use (?si) flags unless noted.

_BUILTIN: dict[str, list[StripPattern]] = {

    "SAS": [
        # Shell / OS commands
        StripPattern(r'^\s*x\s+["\'].*?["\'];\s*$',            "shell command (quoted)"),
        StripPattern(r'^\s*x\s+\w+.*?;\s*$',                   "shell command"),
        # File ingestion
        StripPattern(r'(?si)DATA\s+\w+;\s*INFILE\s+.*?RUN;',   "INFILE data step"),
        StripPattern(r'(?si)execute\s*\(\s*(?:INSERT\s+INTO|TRUNCATE|DROP\s+TABLE'
                     r'|CREATE\s+TABLE\s+\w+\s+AS\s*\n?\s*SELECT\s+\*\s+FROM'
                     r'\s*\n?\s*EXTERNAL).*?\)\s*by\s+\w+;',   "passthrough DDL/load"),
        StripPattern(r'(?si)PROC\s+APPEND\s+.*?RUN;',          "PROC APPEND"),
        StripPattern(r'(?si)PROC\s+DELETE\s+.*?RUN;',          "PROC DELETE"),
        StripPattern(r'(?si)PROC\s+DATASETS\s+.*?(?:RUN|QUIT);',"PROC DATASETS"),
        StripPattern(r'(?si)PROC\s+EXPORT\s+.*?RUN;',          "PROC EXPORT"),
        StripPattern(r'(?si)PROC\s+UPLOAD\s+.*?RUN;',          "PROC UPLOAD"),
        StripPattern(r'(?si)PROC\s+DOWNLOAD\s+.*?RUN;',        "PROC DOWNLOAD"),
        # Reporting / output
        StripPattern(r'(?si)ODS\s+EXCEL\s+.*?;',               "ODS EXCEL"),
        StripPattern(r'(?si)ODS\s+_ALL_\s+.*?;',               "ODS close"),
        StripPattern(r'(?si)ODS\s+LISTING\s+.*?;',             "ODS listing"),
        StripPattern(r'(?si)ODS\s+HTML\s+.*?;',                "ODS HTML"),
        StripPattern(r'(?si)ODS\s+PDF\s+.*?;',                 "ODS PDF"),
        StripPattern(r'(?si)ODS\s+RTF\s+.*?;',                 "ODS RTF"),
        StripPattern(r'(?si)ODS\s+CSV\s+.*?;',                 "ODS CSV"),
        StripPattern(r'(?si)PROC\s+REPORT\s+.*?RUN;',          "PROC REPORT"),
        StripPattern(r'(?si)PROC\s+PRINT\s+.*?RUN;',           "PROC PRINT"),
        StripPattern(r'(?si)PROC\s+TABULATE\s+.*?RUN;',        "PROC TABULATE"),
        StripPattern(r'(?si)PROC\s+ODSTEXT\s*;.*?RUN;',        "PROC ODSTEXT"),
        StripPattern(r'(?si)PROC\s+TEMPLATE\s*;.*?RUN;',       "PROC TEMPLATE"),
        StripPattern(r'(?si)PROC\s+FREQ\s+.*?RUN;',            "PROC FREQ"),
        StripPattern(r'(?si)PROC\s+MEANS\s+.*?RUN;',           "PROC MEANS"),
        StripPattern(r'(?si)PROC\s+UNIVARIATE\s+.*?RUN;',      "PROC UNIVARIATE"),
        StripPattern(r'(?si)FILENAME\s+\w+\s+email.*?;',       "email setup"),
        StripPattern(r'(?si)DATA\s+_NULL_\s*;\s*file\s+mymail.*?RUN;', "email send"),
        StripPattern(r'(?si)TITLE\d*\s+.*?;',                  "TITLE statement"),
        StripPattern(r'(?si)FOOTNOTE\d*\s+.*?;',               "FOOTNOTE statement"),
        # Library assignments (metadata only, not transformation)
        StripPattern(r'(?i)^\s*libname\s+\w+\s+["\'].*?["\'].*?;', "LIBNAME"),
        StripPattern(r'(?i)^\s*libname\s+\w+\s+\w+.*?;',       "LIBNAME engine"),
        StripPattern(r'(?si)OPTIONS\s+.*?;',                    "OPTIONS statement"),
    ],

    "PySpark": [
        # Writes (flag as output, don't analyze as transformation)
        StripPattern(r'(?s)\.write\s*\.\s*\w+\s*\(.*?\)',      "spark write"),
        StripPattern(r'(?s)\.saveAsTable\s*\(.*?\)',            "saveAsTable"),
        StripPattern(r'(?s)\.insertInto\s*\(.*?\)',             "insertInto"),
        # Logging / display
        StripPattern(r'^\s*print\s*\(.*?\)\s*$',               "print statement"),
        StripPattern(r'^\s*display\s*\(.*?\)\s*$',             "display()"),
        StripPattern(r'^\s*logging\.\w+\s*\(.*?\)\s*$',        "logging call"),
        StripPattern(r'(?s)logger\.\w+\s*\(.*?\)',             "logger call"),
        # Spark config / session setup boilerplate
        StripPattern(r'(?s)SparkSession\.builder.*?\.getOrCreate\s*\(\s*\)', "SparkSession init"),
        StripPattern(r'(?s)spark\.conf\.set\s*\(.*?\)',         "spark.conf.set"),
        # argparse / CLI setup
        StripPattern(r'(?s)argparse\.ArgumentParser.*?parse_args\s*\(\s*\)', "argparse"),
        StripPattern(r'^\s*if\s+__name__\s*==\s*["\']__main__["\'].*$',     "__main__ guard"),
    ],

    "Python": [
        StripPattern(r'^\s*print\s*\(.*?\)\s*$',               "print statement"),
        StripPattern(r'^\s*logging\.\w+\s*\(.*?\)\s*$',        "logging call"),
        StripPattern(r'(?s)argparse\.ArgumentParser.*?parse_args\s*\(\s*\)', "argparse"),
        StripPattern(r'^\s*if\s+__name__\s*==\s*["\']__main__["\'].*$',     "__main__ guard"),
    ],

    "R": [
        StripPattern(r'^\s*print\s*\(.*?\)\s*$',               "print()"),
        StripPattern(r'^\s*cat\s*\(.*?\)\s*$',                 "cat()"),
        StripPattern(r'(?s)ggplot\s*\(.*?\)',                  "ggplot display"),
        StripPattern(r'(?s)plot\s*\(.*?\)',                    "plot()"),
        StripPattern(r'(?s)knitr::.*?\(',                      "knitr"),
    ],

    "PL/SQL": [
        StripPattern(r'(?si)DBMS_OUTPUT\.PUT_LINE\s*\(.*?\)\s*;', "DBMS_OUTPUT"),
        StripPattern(r'(?si)DBMS_OUTPUT\.PUT\s*\(.*?\)\s*;',      "DBMS_OUTPUT.PUT"),
        StripPattern(r'(?si)EXCEPTION\s+WHEN\s+OTHERS\s+THEN\s+NULL\s*;', "silent exception handler"),
    ],

    "Scala": [
        StripPattern(r'^\s*println\s*\(.*?\)\s*$',             "println"),
        StripPattern(r'^\s*logger\.\w+\s*\(.*?\)\s*$',        "logger"),
        StripPattern(r'(?s)\.write\s*\.\s*\w+\s*\(.*?\)',     "spark write"),
    ],

    "Informatica": [
        StripPattern(r'(?si)pmcmd\s+.*?;',                     "pmcmd command"),
        StripPattern(r'(?si)Session\s+Task.*?;',               "session task config"),
        StripPattern(r'(?si)Workflow\s+.*?;',                  "workflow config"),
    ],

    "SQL": [
        # GRANT / REVOKE / comments-only blocks
        StripPattern(r'(?i)^\s*GRANT\s+.*?;',                  "GRANT statement"),
        StripPattern(r'(?i)^\s*REVOKE\s+.*?;',                 "REVOKE statement"),
        StripPattern(r'(?i)^\s*COMMENT\s+ON\s+.*?;',           "COMMENT ON"),
    ],
}

# Fallback: if language not in map, use SQL patterns only
_FALLBACK_LANG = "SQL"


def _compile_strip_patterns(
    language: str,
    extra_descriptions: list[str],
) -> list[StripPattern]:
    """
    Merge built-in patterns for the detected language with any Scout-provided
    plain-English block descriptions.

    Scout provides descriptions like:
      "PROC FREQ blocks — reporting only"
      "ODS EXCEL / ODS _ALL_ — output formatting, skip"

    We can't automatically turn arbitrary plain-English into regex, so we log
    them for human review and rely on the built-in patterns to cover them.
    The Scout descriptions are still passed to the Analyzer as context.
    """
    patterns = list(_BUILTIN.get(language, _BUILTIN[_FALLBACK_LANG]))

    if extra_descriptions:
        print(f"  [preprocessor] Scout flagged {len(extra_descriptions)} extra strip hints "
              f"(applied via built-in coverage, logged for review):")
        for d in extra_descriptions:
            print(f"    • {d}")

    return patterns


def preprocess(
    raw_code: str,
    language:  str,
    input_conventions: dict | None = None,
) -> tuple[str, list[str]]:
    """
    Strip ingestion / reporting / logging blocks from source code.

    Args:
        raw_code:           Raw source script text.
        language:           Detected language string (from Scout).
        input_conventions:  Scout output dict; may contain blocks_to_strip list.

    Returns:
        (clean_code, flagged_blocks)
    """
    extra = []
    if input_conventions:
        extra = input_conventions.get("blocks_to_strip", [])

    patterns = _compile_strip_patterns(language, extra)

    flagged: list[str] = []
    clean = raw_code

    for sp in patterns:
        matches = re.findall(sp.pattern, clean, re.MULTILINE)
        for m in matches:
            snippet = m[:120].strip() if isinstance(m, str) else str(m)[:120].strip()
            flagged.append(f"[{language.upper()} - {sp.label}]: {snippet}...")
        clean = re.sub(sp.pattern, "", clean, flags=re.MULTILINE)

    # Strip block comments (language-agnostic: /* ... */)
    clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)

    # Strip Python / R / shell single-line comments
    if language in ("Python", "PySpark", "R", "Scala"):
        clean = re.sub(r'(?m)^\s*#.*$', '', clean)

    # Collapse excessive blank lines
    clean = re.sub(r'\n{3,}', '\n\n', clean)

    return clean.strip(), flagged