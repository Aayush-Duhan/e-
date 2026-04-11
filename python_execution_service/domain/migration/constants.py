"""Migration-stage metadata and labels."""

STEP_LABELS: dict[str, str] = {
    "init_project": "Initialize project",
    "add_source_code": "Ingest source SQL",
    "apply_schema_mapping": "Apply schema mapping",
    "convert_code": "Convert SQL",
    "execute_sql": "Execute SQL",
    "self_heal": "Self-heal fixes",
    "validate": "Validate output",
    "human_review": "Human review",
    "finalize": "Finalize output",
}

THINKING_STEP_IDS: set[str] = {"self_heal", "convert_code", "validate"}
