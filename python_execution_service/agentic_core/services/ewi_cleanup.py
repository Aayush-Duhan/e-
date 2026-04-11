"""Deterministic cleanup of SnowConvert EWI markers from converted SQL.

The SCAI CLI (SnowConvert) adds !!!RESOLVE EWI!!! markers wherever it
encounters unsupported constructs (mainly CHECK constraints). These markers
cause Snowflake syntax errors. This module strips them automatically as a
post-conversion step — no LLM needed.

Handled patterns:
  A) Inline:  ...!!!RESOLVE EWI!!! /*** ... ***/!!! CHECK (...),
  B) Multi-line: marker on one line, CHECK on the next
  C) Standalone CONSTRAINT with marker on its own line
"""

from __future__ import annotations

import re
import logging
from typing import List

logger = logging.getLogger(__name__)


def _strip_all_ewi_tokens(sql: str) -> str:
    """Strip all !!!RESOLVE EWI!!! /*** ... ***/!!! tokens from SQL.

    Handles:
      - Inline markers within code (preserves surrounding text)
      - Whole-line markers (removes the entire line)
    """
    # Remove inline markers: !!!RESOLVE EWI!!! /*** ... ***/!!!
    sql = re.sub(r'!!!RESOLVE EWI!!!\s*/\*\*\*.*?\*\*\*/!!!', '', sql, flags=re.DOTALL)
    # Remove any leftover !!!RESOLVE EWI!!! without the block comment form
    sql = re.sub(r'!!!RESOLVE EWI!!!', '', sql)
    # Remove lines that are now empty (only whitespace) after marker removal
    lines = sql.splitlines()
    cleaned = [line for line in lines if line.strip()]
    return "\n".join(cleaned)


def _fix_udf_helper_references(sql: str) -> str:
    """Replace SnowConvert helper UDF calls with native Snowflake equivalents.

    SnowConvert emits JavaScript helper UDFs (e.g. REGEXP_LIKE_UDF) but
    references them as PUBLIC.<name> in the converted code. These JavaScript
    helpers cannot be called from LANGUAGE SQL functions/procedures.

    This function:
      1. Removes the PUBLIC. schema prefix from helper references.
      2. Replaces helper UDF calls with their native Snowflake equivalents
         so they work inside SQL UDFs without needing the JavaScript helper.
    """
    # Remove PUBLIC. schema prefix from helper UDF references
    sql = re.sub(r'\bPUBLIC\.(\w+_UDF)\b', r'\1', sql)
    # Replace REGEXP_LIKE_UDF(col, pattern) with native REGEXP_LIKE(col, pattern)
    sql = re.sub(r'\bREGEXP_LIKE_UDF\b', 'REGEXP_LIKE', sql)
    return sql


def clean_ewi_markers(sql: str) -> str:
    """Remove all EWI markers and their associated CHECK clauses.

    This is a multi-pass approach:
      1. Strip all !!!RESOLVE EWI!!! /*** ... ***/!!! tokens globally
      2. Remove inline EWI marker + CHECK on the same line
      3. Remove multi-line EWI marker line + following CHECK line
      4. Remove standalone EWI marker lines (no CHECK follows)
      5. Clean up residual empty lines and fix trailing commas

    Args:
        sql: Raw converted SQL from SnowConvert.

    Returns:
        Cleaned SQL ready for Snowflake execution.
    """
    if not sql:
        return sql

    # Always fix UDF helper schema references
    sql = _fix_udf_helper_references(sql)

    if "!!!RESOLVE EWI!!!" not in sql:
        return sql

    # --- Pass 0: Strip all EWI tokens (handles inline, standalone, inside $$) ---
    sql = _strip_all_ewi_tokens(sql)

    # If no markers remain after the global strip, do final cleanup and return
    if "!!!RESOLVE EWI!!!" not in sql:
        sql = re.sub(r',(\s*\n\s*\))', r'\1', sql)
        sql = re.sub(r'\n{3,}', '\n\n', sql)
        return sql

    lines = sql.splitlines()
    cleaned_lines: List[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # --- Check if this line contains an EWI marker ---
        if "!!!RESOLVE EWI!!!" in line:
            # Determine what to do based on whether CHECK follows

            # Pattern A: Inline — marker and CHECK on same line
            # e.g.: "  !!!RESOLVE EWI!!! /*** ... ***/!!! CHECK (ACTIVE_FLAG IN ('Y','N')),"
            inline_match = re.search(
                r'!!!RESOLVE EWI!!!.*?!!!\s*(?:CONSTRAINT\s+\w+\s+)?CHECK\s*\(', line
            )

            if inline_match:
                # Remove everything from the marker to end of line
                # But preserve any leading content before the marker
                prefix = line[:line.index("!!!RESOLVE EWI!!!")]
                prefix = prefix.rstrip()

                # Determine if the CHECK clause ended with a comma
                check_end = _find_check_end(line, inline_match.start())
                rest_after_check = line[check_end:].strip() if check_end < len(line) else ""
                had_trailing_comma = rest_after_check.startswith(",")

                if prefix:
                    # There's real SQL before the marker (e.g., column definition)
                    if had_trailing_comma:
                        cleaned_lines.append(prefix + ",")
                    elif rest_after_check:
                        cleaned_lines.append(prefix + " " + rest_after_check)
                    else:
                        cleaned_lines.append(prefix)
                else:
                    # Entire line is EWI marker + CHECK — skip it, but
                    # if this line had a trailing comma, the PREVIOUS line
                    # needs that comma (it's the column separator)
                    if had_trailing_comma and cleaned_lines:
                        prev = cleaned_lines[-1].rstrip()
                        if not prev.endswith(","):
                            cleaned_lines[-1] = prev + ","
                i += 1
                continue

            # Pattern B/C: Marker on its own line (no CHECK on this line)
            # Check if CHECK is on the NEXT line
            if i + 1 < len(lines):
                next_line_stripped = lines[i + 1].strip()
                if re.match(r'(?:CONSTRAINT\s+\w+\s+)?CHECK\s*\(', next_line_stripped):
                    # The CHECK line may end with a comma — preserve it
                    next_line_raw = lines[i + 1]
                    check_end_next = _find_check_end(next_line_raw, 0)
                    rest_after = next_line_raw[check_end_next:].strip() if check_end_next < len(next_line_raw) else ""
                    had_comma = rest_after.startswith(",")

                    if had_comma and cleaned_lines:
                        prev = cleaned_lines[-1].rstrip()
                        if not prev.endswith(","):
                            cleaned_lines[-1] = prev + ","

                    i += 2  # skip marker + CHECK
                    continue

            # Pattern: Standalone marker line (no CHECK follows) — just remove it
            i += 1
            continue

        cleaned_lines.append(line)
        i += 1

    result = "\n".join(cleaned_lines)

    # --- Final cleanup pass ---
    # Remove any leftover dangling commas before closing parenthesis
    # e.g., "    CONSTRAINT PK_...,\n)" → "    CONSTRAINT PK_...\n)"
    result = re.sub(r',(\s*\n\s*\))', r'\1', result)

    # Remove consecutive blank lines (more than 2 in a row)
    result = re.sub(r'\n{3,}', '\n\n', result)

    return result


def _find_check_end(line: str, start_pos: int) -> int:
    """Find the end position of a CHECK(...) clause in a line.

    Handles nested parentheses correctly.
    """
    # Find the opening paren of CHECK
    paren_start = line.find("(", start_pos)
    if paren_start == -1:
        return len(line)

    depth = 0
    for pos in range(paren_start, len(line)):
        if line[pos] == "(":
            depth += 1
        elif line[pos] == ")":
            depth -= 1
            if depth == 0:
                return pos + 1
    return len(line)


def clean_ewi_from_file(file_path: str) -> bool:
    """Clean EWI markers from a file in-place.

    Args:
        file_path: Path to the SQL file.

    Returns:
        True if the file was modified, False if no changes were needed.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            original = f.read()

        content = original
        if content.startswith('\ufeff'):
            content = content[1:]

        cleaned = clean_ewi_markers(content)

        if cleaned != original:
            with open(file_path, "w", encoding="utf-8", newline="") as f:
                f.write(cleaned)
            logger.info("Cleaned EWI markers from %s", file_path)
            return True

        return False
    except Exception as exc:
        logger.error("Failed to clean EWI markers from %s: %s", file_path, exc)
        return False
