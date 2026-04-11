import csv
import difflib
import json
import os
import re



def _strip_sql_server_brackets(sql: str) -> str:
    """Remove SQL Server bracket delimiters from identifiers.

    Converts [identifier] to identifier. Only strips brackets around
    valid SQL Server identifier characters to avoid false positives.
    """
    return re.sub(r"\[([A-Za-z_][A-Za-z0-9_ $#]*)\]", r"\1", sql)


def _classify_mapping_rows(original_sql, mapping_rows):
    names_followed_by_dot = {
        match.group(1).upper()
        for match in re.finditer(
            r"\b([A-Za-z_][A-Za-z0-9_$#]*)\b(?=\s*\.)",
            original_sql,
        )
    }

    schema_entries = []
    table_entries = []

    for old_schema, new_db_schema in mapping_rows:
        if old_schema.upper() in names_followed_by_dot:
            schema_entries.append((old_schema, new_db_schema))
        else:
            table_entries.append((old_schema, new_db_schema))

    schema_entries.sort(key=lambda item: len(item[0]), reverse=True)
    table_entries.sort(key=lambda item: len(item[0]), reverse=True)
    return schema_entries, table_entries


def _resolve_markers(sql, markers):
    for marker, final_value in markers.items():
        sql = sql.replace(marker, final_value)
    return sql


def _apply_schema_mapping_to_sql(original_sql, mapping_rows):
    if not mapping_rows:
        return original_sql, 0, 0

    # Strip SQL Server bracket delimiters ([identifier] -> identifier)
    # before applying mapping so that \b word-boundary patterns match correctly.
    sql = _strip_sql_server_brackets(original_sql)

    schema_entries, table_entries = _classify_mapping_rows(sql, mapping_rows)
    total_matches = 0
    total_replacements = 0
    markers = {}
    marker_index = 0

    def make_marker(replacement_text):
        nonlocal marker_index
        marker = f"__SMAP_{marker_index}__"
        marker_index += 1
        markers[marker] = replacement_text
        return marker

    def apply_combined_pattern(sql_text, entries, pattern_template):
        nonlocal total_matches, total_replacements
        if not entries:
            return sql_text

        replacement_by_name = {
            source.upper(): make_marker(target)
            for source, target in entries
        }
        pattern = re.compile(
            pattern_template.format(
                names="|".join(re.escape(source) for source, _ in entries),
            ),
            flags=re.IGNORECASE,
        )

        def replace_match(match):
            nonlocal total_matches, total_replacements
            total_matches += 1
            total_replacements += 1
            return replacement_by_name[match.group("name").upper()]

        return pattern.sub(replace_match, sql_text)

    sql = apply_combined_pattern(
        sql,
        schema_entries,
        r"(?<!\.)(?P<name>\b(?:{names})\b)(?=\s*\.)",
    )
    sql = apply_combined_pattern(
        sql,
        table_entries,
        r"(?<!\.)(?P<name>\b(?:{names})\b)",
    )
    return _resolve_markers(sql, markers), total_matches, total_replacements


def _read_mapping_rows(csv_file_path):
    mapping_rows = []
    with open(csv_file_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            old_schema = str(row.get("SOURCE_SCHEMA", "")).strip()
            new_db_schema = str(row.get("TARGET_DB_SCHEMA", "")).strip()
            if old_schema and new_db_schema:
                mapping_rows.append((old_schema, new_db_schema))
    return mapping_rows


def process_sql_with_pandas_replace(
    csv_file_path,
    sql_file_path,
    output_dir,
):
    summary_data = {}
    mapping_rows = _read_mapping_rows(csv_file_path)

    for filename in os.listdir(sql_file_path):
        summary_file_data = []
        if filename.endswith((".sql", ".btq", ".ddl")):
            print(filename)
            file_path = os.path.join(sql_file_path, filename)
            with open(file_path, "r", encoding="utf-8-sig") as sql_file:
                original_sql = sql_file.read()
                before_change_file = original_sql

            os.makedirs(output_dir, exist_ok=True)

            result_sql, total_num_matches, total_num_replacements = _apply_schema_mapping_to_sql(
                original_sql,
                mapping_rows,
            )
            original_sql = result_sql

            summary_file_data.append(f"Name of the filename : {filename}")
            summary_file_data.append(f"No of places changes expected : {total_num_matches}")
            after_change_file = original_sql

            before_proc_lines = before_change_file.strip().splitlines()
            after_proc_lines = after_change_file.strip().splitlines()
            diff = difflib.unified_diff(
                before_proc_lines,
                after_proc_lines,
                fromfile="before_change_file",
                tofile="after_change_file",
                lineterm="",
            )

            before_lines = []
            after_lines = []
            for line in diff:
                if line.startswith("-") and not line.startswith("---"):
                    before_lines.append(line[1:].strip())
                elif line.startswith("+") and not line.startswith("+++"):
                    after_lines.append(line[1:].strip())

            sp_count = 0
            inside_db_count = 0
            for before, after in zip(before_lines, after_lines):
                sp_string = "REPLACE PROCEDURE"
                if (sp_string in before) and (sp_string in after):
                    if before != after:
                        sp_count += 1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            total_num_replacements -= 1
                            summary_file_data.append("SP DB Change: NO")
                        else:
                            summary_file_data.append("SP DB Change: YES")
                else:
                    if before != after:
                        inside_db_count += 1
                        if "DB_NOT_FOUND.SCHEMA_NOT_FOUND" in after:
                            total_num_replacements -= 1

            if sp_count == 0:
                summary_file_data.append("SP DB Change: NO")

            summary_file_data.append(f"No of places changes implemented: {total_num_replacements}")
            summary_data[filename] = summary_file_data

            if filename.endswith(".btq"):
                filename = filename.replace(".btq", ".sql")
            elif filename.endswith(".ddl"):
                filename = filename.replace(".ddl", ".sql")

            output_file = os.path.join(output_dir, f"{filename}")
            with open(output_file, "w", encoding="utf-8", newline="") as output_sql_file:
                output_sql_file.write(original_sql)

            print(f"Saved updated SQL to {output_file}")

    summary_json_file_name = "summary.json"
    with open(summary_json_file_name, "w", encoding="utf-8", newline="") as json_file:
        json.dump(summary_data, json_file, indent=4)
