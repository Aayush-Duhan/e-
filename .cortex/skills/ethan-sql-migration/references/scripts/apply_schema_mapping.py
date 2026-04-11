"""
apply_schema_mapping.py - Apply schema mapping CSV to SQL files.

Usage:
    python apply_schema_mapping.py <csv_path> <sql_directory>

The CSV must have columns: SOURCE_SCHEMA, TARGET_DB_SCHEMA

Replacement rules:
    - Schema entries (SOURCE followed by '.' in SQL):
        ODS.TABLE_NAME  ->  DBMIG_POC.DBMIG_POC.TABLE_NAME
    - Standalone/table entries (SOURCE not followed by '.'):
        CUSTOMERS  ->  DBMIG_POC.DBMIG_POC.CUSTOMERS
    - Longer names are processed first to avoid partial-match conflicts.
"""

import csv
import re
import sys
import os
import glob


def _strip_sql_server_brackets(sql):
    """Remove SQL Server bracket delimiters from identifiers.

    Converts [identifier] to identifier. Only strips brackets around
    valid SQL Server identifier characters to avoid false positives.
    """
    return re.sub(r"\[([A-Za-z_][A-Za-z0-9_ $#]*)\]", r"\1", sql)


def load_mappings(csv_path):
    """Load and return mappings sorted by source name length descending."""
    mappings = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row["SOURCE_SCHEMA"].strip()
            target = row["TARGET_DB_SCHEMA"].strip()
            if source and target:
                mappings.append((source, target))
    # Sort by length descending to avoid partial matches
    mappings.sort(key=lambda x: len(x[0]), reverse=True)
    return mappings


def apply_mappings(content, mappings):
    """Apply all mappings to SQL content. Returns (new_content, replacement_count)."""
    # Strip SQL Server bracket delimiters ([identifier] -> identifier)
    # before applying mapping so that \b word-boundary patterns match correctly.
    content = _strip_sql_server_brackets(content)
    total_replacements = 0

    for source, target in mappings:
        # Pattern 1: Schema reference - SOURCE followed by '.'
        schema_pattern = re.compile(
            r"\b" + re.escape(source) + r"\.",
            re.IGNORECASE
        )
        matches = schema_pattern.findall(content)
        if matches:
            total_replacements += len(matches)
            content = schema_pattern.sub(target + ".", content)

        # Pattern 2: Standalone reference - SOURCE not preceded/followed by '.'
        # Only apply if target contains more dots than source (table-level mapping)
        if target.count(".") >= 2:
            standalone_pattern = re.compile(
                r"(?<!\.)\b" + re.escape(source) + r"\b(?!\.)",
                re.IGNORECASE
            )
            matches = standalone_pattern.findall(content)
            if matches:
                total_replacements += len(matches)
                content = standalone_pattern.sub(target, content)

    return content, total_replacements


def main():
    if len(sys.argv) != 3:
        print("Usage: python apply_schema_mapping.py <csv_path> <sql_directory>")
        sys.exit(1)

    csv_path = sys.argv[1]
    sql_dir = sys.argv[2]

    if not os.path.isfile(csv_path):
        print(f"Error: CSV file '{csv_path}' not found.")
        sys.exit(1)
    if not os.path.isdir(sql_dir):
        print(f"Error: Directory '{sql_dir}' not found.")
        sys.exit(1)

    mappings = load_mappings(csv_path)
    print(f"Loaded {len(mappings)} mapping(s) from {os.path.basename(csv_path)}:")
    for source, target in mappings:
        print(f"  {source} -> {target}")
    print()

    sql_files = glob.glob(os.path.join(sql_dir, "**", "*.sql"), recursive=True)
    if not sql_files:
        print(f"No .sql files found in '{sql_dir}'.")
        sys.exit(0)

    total_replacements = 0
    files_modified = 0

    for filepath in sql_files:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        new_content, count = apply_mappings(content, mappings)

        if count > 0:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(new_content)
            files_modified += 1
            total_replacements += count
            print(f"  {count} replacement(s) in {os.path.basename(filepath)}")

    print(f"\nSummary: {total_replacements} replacement(s) across {files_modified} file(s) ({len(sql_files)} file(s) scanned).")


if __name__ == "__main__":
    main()
