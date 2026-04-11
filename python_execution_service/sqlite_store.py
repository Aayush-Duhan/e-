import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT_DIR / "data" / "app.db"
SCHEMA_PATH = ROOT_DIR / "db" / "schema_v1.sql"


def _db_path() -> Path:
    configured = os.getenv("APP_SQLITE_PATH", "").strip()
    if configured:
        return Path(configured).resolve()
    return DEFAULT_DB_PATH.resolve()


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    db_path = _db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_schema() -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect() as conn:
        conn.executescript(sql)
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(runs)").fetchall()
        }
        if "missing_objects_json" not in columns:
            conn.execute(
                "ALTER TABLE runs ADD COLUMN missing_objects_json TEXT NOT NULL DEFAULT '[]'"
            )
        conn.execute(
            """
            INSERT OR REPLACE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            ("v1", _now_iso()),
        )


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_load(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return fallback


def save_run_snapshot(run: dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO runs(
                run_id, project_id, project_name, source_id, schema_id,
                source_language, source_path, schema_path, status, created_at,
                updated_at, error, sf_account, sf_user, sf_role, sf_warehouse,
                sf_database, sf_schema, sf_authenticator, requires_ddl_upload,
                resume_from_stage, last_executed_file_index, self_heal_iteration,
                missing_objects_json, output_dir, ddl_upload_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                project_id = excluded.project_id,
                project_name = excluded.project_name,
                source_id = excluded.source_id,
                schema_id = excluded.schema_id,
                source_language = excluded.source_language,
                source_path = excluded.source_path,
                schema_path = excluded.schema_path,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                error = excluded.error,
                sf_account = excluded.sf_account,
                sf_user = excluded.sf_user,
                sf_role = excluded.sf_role,
                sf_warehouse = excluded.sf_warehouse,
                sf_database = excluded.sf_database,
                sf_schema = excluded.sf_schema,
                sf_authenticator = excluded.sf_authenticator,
                requires_ddl_upload = excluded.requires_ddl_upload,
                resume_from_stage = excluded.resume_from_stage,
                last_executed_file_index = excluded.last_executed_file_index,
                self_heal_iteration = excluded.self_heal_iteration,
                missing_objects_json = excluded.missing_objects_json,
                output_dir = excluded.output_dir,
                ddl_upload_path = excluded.ddl_upload_path
            """,
            (
                run["runId"],
                run["projectId"],
                run["projectName"],
                run.get("sourceId", ""),
                run.get("schemaId"),
                run.get("sourceLanguage", "teradata"),
                run.get("sourcePath", ""),
                run.get("schemaPath", ""),
                run.get("status", "failed"),
                run.get("createdAt", _now_iso()),
                run.get("updatedAt", _now_iso()),
                run.get("error"),
                run.get("sfAccount"),
                run.get("sfUser"),
                run.get("sfRole"),
                run.get("sfWarehouse"),
                run.get("sfDatabase"),
                run.get("sfSchema"),
                run.get("sfAuthenticator"),
                1 if run.get("requiresDdlUpload") else 0,
                run.get("resumeFromStage", ""),
                int(run.get("lastExecutedFileIndex", -1)),
                int(run.get("selfHealIteration", 0)),
                _json_dump(run.get("missingObjects", [])),
                run.get("outputDir", ""),
                run.get("ddlUploadPath", ""),
            ),
        )

        conn.execute("DELETE FROM run_steps WHERE run_id = ?", (run["runId"],))
        for step in run.get("steps", []):
            conn.execute(
                """
                INSERT INTO run_steps(run_id, step_id, label, status, started_at, ended_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run["runId"],
                    step.get("id", ""),
                    step.get("label", ""),
                    step.get("status", "pending"),
                    step.get("startedAt"),
                    step.get("endedAt"),
                ),
            )

        conn.execute("DELETE FROM run_validation_issues WHERE run_id = ?", (run["runId"],))
        for item in run.get("validationIssues", []):
            conn.execute(
                "INSERT INTO run_validation_issues(run_id, payload_json) VALUES (?, ?)",
                (run["runId"], _json_dump(item)),
            )

        conn.execute("DELETE FROM run_execution_entries WHERE run_id = ?", (run["runId"],))
        for item in run.get("executionLog", []):
            conn.execute(
                "INSERT INTO run_execution_entries(run_id, entry_type, payload_json) VALUES (?, ?, ?)",
                (run["runId"], "log", _json_dump(item)),
            )
        for item in run.get("executionErrors", []):
            conn.execute(
                "INSERT INTO run_execution_entries(run_id, entry_type, payload_json) VALUES (?, ?, ?)",
                (run["runId"], "error", _json_dump(item)),
            )


def append_run_log(run_id: str, message: str, created_at: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO run_logs(run_id, message, created_at)
            VALUES (?, ?, ?)
            """,
            (run_id, message, created_at),
        )


def append_run_event(run_id: str, event_type: str, payload: dict[str, Any], timestamp: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO run_events(run_id, event_type, payload_json, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, event_type, _json_dump(payload), timestamp),
        )


def append_run_message(run_id: str, message: dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO run_messages(
              run_id, msg_id, ts, role, kind, content, step_json, sql_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                message.get("id", ""),
                message.get("ts", _now_iso()),
                message.get("role", "agent"),
                message.get("kind", "log"),
                message.get("content", ""),
                _json_dump(message["step"]) if "step" in message else None,
                _json_dump(message["sql"]) if "sql" in message else None,
            ),
        )


def update_run_step(run_id: str, step_id: str, status: str, started_at: str | None, ended_at: str | None) -> None:
    """Update a single step's status and timestamps in the DB."""
    with connect() as conn:
        conn.execute(
            "UPDATE run_steps SET status = ?, started_at = COALESCE(?, started_at), ended_at = COALESCE(?, ended_at) WHERE run_id = ? AND step_id = ?",
            (status, started_at, ended_at, run_id, step_id),
        )


def list_runs() -> list[dict[str, Any]]:
    with connect() as conn:
        run_rows = conn.execute(
            """
            SELECT
              run_id, project_id, project_name, source_id, schema_id, source_language,
              source_path, schema_path, status, created_at, updated_at, error,
              sf_account, sf_user, sf_role, sf_warehouse, sf_database, sf_schema,
              sf_authenticator, requires_ddl_upload, resume_from_stage,
              last_executed_file_index, self_heal_iteration, missing_objects_json,
              output_dir, ddl_upload_path
            FROM runs
            """
        ).fetchall()

        if not run_rows:
            return []

        run_ids = [row[0] for row in run_rows]
        placeholders = ",".join("?" for _ in run_ids)

        # Batch-fetch all child rows in bulk (fixes N+1)
        all_steps = conn.execute(
            f"SELECT run_id, step_id, label, status, started_at, ended_at FROM run_steps WHERE run_id IN ({placeholders}) ORDER BY rowid",
            run_ids,
        ).fetchall()
        all_logs = conn.execute(
            f"SELECT run_id, message FROM run_logs WHERE run_id IN ({placeholders}) ORDER BY id",
            run_ids,
        ).fetchall()
        all_events = conn.execute(
            f"SELECT run_id, event_type, payload_json, timestamp FROM run_events WHERE run_id IN ({placeholders}) ORDER BY id",
            run_ids,
        ).fetchall()
        all_messages = conn.execute(
            f"SELECT run_id, msg_id, ts, role, kind, content, step_json, sql_json FROM run_messages WHERE run_id IN ({placeholders}) ORDER BY id",
            run_ids,
        ).fetchall()
        all_validation = conn.execute(
            f"SELECT run_id, payload_json FROM run_validation_issues WHERE run_id IN ({placeholders}) ORDER BY id",
            run_ids,
        ).fetchall()
        all_execution = conn.execute(
            f"SELECT run_id, entry_type, payload_json FROM run_execution_entries WHERE run_id IN ({placeholders}) ORDER BY id",
            run_ids,
        ).fetchall()

        # Group by run_id
        from collections import defaultdict
        steps_by_run: dict[str, list] = defaultdict(list)
        for r in all_steps:
            steps_by_run[r[0]].append(r[1:])
        logs_by_run: dict[str, list] = defaultdict(list)
        for r in all_logs:
            logs_by_run[r[0]].append(r[1])
        events_by_run: dict[str, list] = defaultdict(list)
        for r in all_events:
            events_by_run[r[0]].append(r[1:])
        messages_by_run: dict[str, list] = defaultdict(list)
        for r in all_messages:
            messages_by_run[r[0]].append(r[1:])
        validation_by_run: dict[str, list] = defaultdict(list)
        for r in all_validation:
            validation_by_run[r[0]].append(r[1])
        execution_by_run: dict[str, list] = defaultdict(list)
        for r in all_execution:
            execution_by_run[r[0]].append(r[1:])

        result: list[dict[str, Any]] = []
        for row in run_rows:
            run_id = row[0]

            execution_log: list[dict[str, Any]] = []
            execution_errors: list[dict[str, Any]] = []
            for entry_type, payload_json in execution_by_run.get(run_id, []):
                item = _json_load(payload_json, {})
                if entry_type == "error":
                    execution_errors.append(item)
                else:
                    execution_log.append(item)

            result.append(
                {
                    "runId": run_id,
                    "projectId": row[1],
                    "projectName": row[2],
                    "sourceId": row[3],
                    "schemaId": row[4] or "",
                    "sourceLanguage": row[5] or "teradata",
                    "sourcePath": row[6] or "",
                    "schemaPath": row[7] or "",
                    "status": row[8] or "failed",
                    "createdAt": row[9] or _now_iso(),
                    "updatedAt": row[10] or _now_iso(),
                    "error": row[11],
                    "sfAccount": row[12],
                    "sfUser": row[13],
                    "sfRole": row[14],
                    "sfWarehouse": row[15],
                    "sfDatabase": row[16],
                    "sfSchema": row[17],
                    "sfAuthenticator": row[18],
                    "requiresDdlUpload": bool(row[19]),
                    "resumeFromStage": row[20] or "",
                    "lastExecutedFileIndex": int(row[21] if row[21] is not None else -1),
                    "selfHealIteration": int(row[22] if row[22] is not None else 0),
                    "missingObjects": _json_load(row[23] or "[]", []),
                    "outputDir": row[24] or "",
                    "ddlUploadPath": row[25] or "",
                    "steps": [
                        {
                            "id": step[0],
                            "label": step[1],
                            "status": step[2],
                            "startedAt": step[3],
                            "endedAt": step[4],
                        }
                        for step in steps_by_run.get(run_id, [])
                    ],
                    "logs": list(logs_by_run.get(run_id, [])),
                    "events": [
                        {
                            "type": event[0],
                            "payload": _json_load(event[1], {}),
                            "timestamp": event[2],
                        }
                        for event in events_by_run.get(run_id, [])
                    ],
                    "messages": [
                        {
                            "id": msg[0],
                            "ts": msg[1],
                            "role": msg[2],
                            "kind": msg[3],
                            "content": msg[4],
                            **(
                                {"step": _json_load(msg[5], {})}
                                if msg[5]
                                else {}
                            ),
                            **(
                                {"sql": _json_load(msg[6], {})}
                                if msg[6]
                                else {}
                            ),
                        }
                        for msg in messages_by_run.get(run_id, [])
                    ],
                    "validationIssues": [
                        _json_load(issue, {}) for issue in validation_by_run.get(run_id, [])
                    ],
                    "executionLog": execution_log,
                    "executionErrors": execution_errors,
                }
            )
    return result


# ── RunStore: centralized DB-backed run access ──────────────────

# Column mapping: SQLite snake_case -> RunRecord camelCase
_RUN_COLUMNS = (
    "run_id", "project_id", "project_name", "source_id", "schema_id",
    "source_language", "source_path", "schema_path", "status", "created_at",
    "updated_at", "error", "sf_account", "sf_user", "sf_role", "sf_warehouse",
    "sf_database", "sf_schema", "sf_authenticator", "requires_ddl_upload",
    "resume_from_stage", "last_executed_file_index", "self_heal_iteration",
    "missing_objects_json", "output_dir", "ddl_upload_path",
)

_RUN_CAMEL = (
    "runId", "projectId", "projectName", "sourceId", "schemaId",
    "sourceLanguage", "sourcePath", "schemaPath", "status", "createdAt",
    "updatedAt", "error", "sfAccount", "sfUser", "sfRole", "sfWarehouse",
    "sfDatabase", "sfSchema", "sfAuthenticator", "requiresDdlUpload",
    "resumeFromStage", "lastExecutedFileIndex", "selfHealIteration",
    "missingObjects", "outputDir", "ddlUploadPath",
)

# Mapping from camelCase field name to snake_case DB column
_CAMEL_TO_SNAKE: dict[str, str] = dict(zip(_RUN_CAMEL, _RUN_COLUMNS))


def _row_to_dict(row: tuple) -> dict[str, Any]:
    """Convert a runs table row tuple to a camelCase dict suitable for RunRecord.model_validate."""
    d: dict[str, Any] = {}
    for i, key in enumerate(_RUN_CAMEL):
        val = row[i]
        if key == "requiresDdlUpload":
            val = bool(val)
        elif key == "lastExecutedFileIndex":
            val = int(val) if val is not None else -1
        elif key == "selfHealIteration":
            val = int(val) if val is not None else 0
        elif key == "missingObjects":
            val = _json_load(val or "[]", [])
        d[key] = val
    return d


class RunStore:
    """Centralized DB-backed access layer for run records.

    Provides targeted reads and atomic partial updates so that SQLite
    is the durable source of truth rather than an in-memory dict.
    """

    @staticmethod
    def get_run(run_id: str) -> dict[str, Any] | None:
        """Fetch a single run with all child data, or None if not found."""
        with connect() as conn:
            row = conn.execute(
                f"SELECT {', '.join(_RUN_COLUMNS)} FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                return None

            d = _row_to_dict(row)

            # Fetch child tables
            steps = conn.execute(
                "SELECT step_id, label, status, started_at, ended_at FROM run_steps WHERE run_id = ? ORDER BY rowid",
                (run_id,),
            ).fetchall()
            d["steps"] = [
                {"id": s[0], "label": s[1], "status": s[2], "startedAt": s[3], "endedAt": s[4]}
                for s in steps
            ]

            logs = conn.execute(
                "SELECT message FROM run_logs WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            d["logs"] = [log[0] for log in logs]

            events = conn.execute(
                "SELECT event_type, payload_json, timestamp FROM run_events WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            d["events"] = [
                {"type": e[0], "payload": _json_load(e[1], {}), "timestamp": e[2]}
                for e in events
            ]

            messages = conn.execute(
                "SELECT msg_id, ts, role, kind, content, step_json, sql_json FROM run_messages WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            d["messages"] = [
                {
                    "id": m[0], "ts": m[1], "role": m[2], "kind": m[3], "content": m[4],
                    **({"step": _json_load(m[5], {})} if m[5] else {}),
                    **({"sql": _json_load(m[6], {})} if m[6] else {}),
                }
                for m in messages
            ]

            validation = conn.execute(
                "SELECT payload_json FROM run_validation_issues WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            d["validationIssues"] = [_json_load(v[0], {}) for v in validation]

            entries = conn.execute(
                "SELECT entry_type, payload_json FROM run_execution_entries WHERE run_id = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            d["executionLog"] = [_json_load(e[1], {}) for e in entries if e[0] != "error"]
            d["executionErrors"] = [_json_load(e[1], {}) for e in entries if e[0] == "error"]

            return d

    @staticmethod
    def save_run(run_dict: dict[str, Any]) -> None:
        """Full upsert of a run record and its child tables. Delegates to save_run_snapshot."""
        save_run_snapshot(run_dict)

    @staticmethod
    def update_run_status(run_id: str, status: str, *, error: str | None = None, updated_at: str | None = None) -> None:
        """Atomic status update — only touches the status, error, and updated_at columns."""
        ts = updated_at or _now_iso()
        with connect() as conn:
            conn.execute(
                "UPDATE runs SET status = ?, error = ?, updated_at = ? WHERE run_id = ?",
                (status, error, ts, run_id),
            )

    @staticmethod
    def update_run_fields(run_id: str, updated_at: str | None = None, **fields: Any) -> None:
        """Partial field update — only writes the specified columns.

        Accepts camelCase field names matching RunRecord (e.g. ``requiresDdlUpload``).
        Automatically maps to snake_case DB columns.
        """
        if not fields:
            return
        ts = updated_at or _now_iso()
        sets: list[str] = ["updated_at = ?"]
        params: list[Any] = [ts]
        for camel_key, value in fields.items():
            col = _CAMEL_TO_SNAKE.get(camel_key)
            if not col:
                continue
            if col == "requires_ddl_upload":
                value = 1 if value else 0
            elif col == "missing_objects_json":
                value = _json_dump(value)
            sets.append(f"{col} = ?")
            params.append(value)
        params.append(run_id)
        with connect() as conn:
            conn.execute(
                f"UPDATE runs SET {', '.join(sets)} WHERE run_id = ?",
                params,
            )

    @staticmethod
    def list_runs_summary(
        *,
        limit: int = 100,
        status: str | None = None,
        project_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Lightweight listing: run metadata + steps only (no events/logs/messages)."""
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if project_id:
            clauses.append("project_id = ?")
            params.append(project_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

        with connect() as conn:
            run_rows = conn.execute(
                f"SELECT {', '.join(_RUN_COLUMNS)} FROM runs{where} ORDER BY updated_at DESC LIMIT ?",
                params + [limit],
            ).fetchall()

            if not run_rows:
                return []

            run_ids = [row[0] for row in run_rows]
            placeholders = ",".join("?" for _ in run_ids)

            all_steps = conn.execute(
                f"SELECT run_id, step_id, label, status, started_at, ended_at FROM run_steps WHERE run_id IN ({placeholders}) ORDER BY rowid",
                run_ids,
            ).fetchall()

            from collections import defaultdict
            steps_by_run: dict[str, list] = defaultdict(list)
            for s in all_steps:
                steps_by_run[s[0]].append(s[1:])

            result: list[dict[str, Any]] = []
            for row in run_rows:
                d = _row_to_dict(row)
                d["steps"] = [
                    {"id": s[0], "label": s[1], "status": s[2], "startedAt": s[3], "endedAt": s[4]}
                    for s in steps_by_run.get(d["runId"], [])
                ]
                result.append(d)
            return result
