"""
clean_ewi_fdm.py - Remove EWI and FDM markers from SnowConvert output files.

Usage:
    python clean_ewi_fdm.py <directory>

Removes:
    - Full-line EWI comments:  --** SSC-EWI-...
    - Inline EWI comments:     /*EWI:...*/
    - Inline FDM comments:     /*** SSC-FDM-XXXX - ... ***/
"""

import re
import sys
import os
import glob


# Patterns to remove
FULL_LINE_EWI = re.compile(r"^\s*--\*\*\s*SSC-EWI-.*$", re.MULTILINE)
INLINE_EWI = re.compile(r"\s*/\*EWI:.*?\*/")
INLINE_FDM = re.compile(r"\s*/\*\*\*\s*SSC-FDM-\S+\s+-\s+.*?\*\*\*/")


def clean_file(filepath):
    """Clean EWI/FDM markers from a single file. Returns count of markers removed."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    original = content
    removed = 0

    # Count and remove full-line EWI markers
    matches = FULL_LINE_EWI.findall(content)
    removed += len(matches)
    content = FULL_LINE_EWI.sub("", content)

    # Count and remove inline EWI markers
    matches = INLINE_EWI.findall(content)
    removed += len(matches)
    content = INLINE_EWI.sub("", content)

    # Count and remove inline FDM markers
    matches = INLINE_FDM.findall(content)
    removed += len(matches)
    content = INLINE_FDM.sub("", content)

    # Clean up blank lines left behind (collapse multiple blank lines to one)
    content = re.sub(r"\n{3,}", "\n\n", content)

    if content != original:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

    return removed


def main():
    if len(sys.argv) != 2:
        print("Usage: python clean_ewi_fdm.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.")
        sys.exit(1)

    sql_files = glob.glob(os.path.join(directory, "**", "*.sql"), recursive=True)
    if not sql_files:
        print(f"No .sql files found in '{directory}'.")
        sys.exit(0)

    total_removed = 0
    files_modified = 0

    for filepath in sql_files:
        removed = clean_file(filepath)
        if removed > 0:
            files_modified += 1
            total_removed += removed
            print(f"  Cleaned {removed} marker(s) from {os.path.basename(filepath)}")

    print(f"\nSummary: Removed {total_removed} marker(s) from {files_modified} file(s) ({len(sql_files)} file(s) scanned).")


if __name__ == "__main__":
    main()
