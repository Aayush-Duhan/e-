"""Run lifecycle helpers, persistence sync, and event utilities."""

import json
import logging
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from python_execution_service.infrastructure.persistence.sqlite import store as sqlite_store
from python_execution_service.app.config.settings import EXECUTION_TOKEN
from python_execution_service.domain.migration.constants import STEP_LABELS
from python_execution_service.domain.runs.state import (
    CANCEL_FLAGS,
    RUN_LOCK,
    RUNS,
    USER_MESSAGE_QUEUES,
)
from python_execution_service.shared.models.runs import RunRecord, RunStep, StartRunRequest
from python_execution_service.infrastructure.persistence.sqlite.store import RunStore

logger = logging.getLogger(__name__)


# â”€â”€ Time helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def now_iso() -> str:
    return datetime.utcnow().isoformat()


# â”€â”€ Serialization / deserialization â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _serialize_run_record(run: RunRecord) -> dict[str, Any]:
    return run.model_dump()


def _deserialize_run_record(payload: dict[str, Any]) -> RunRecord:
    return RunRecord.model_validate(payload)


# â”€â”€ Persistence â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def persist_run(run: RunRecord) -> None:
    """Persist a single run snapshot to SQLite."""
    try:
        sqlite_store.save_run_snapshot(_serialize_run_record(run))
    except Exception as exc:
        logger.warning("Failed to persist run %s: %s", run.runId, exc)


def load_persisted_runs() -> None:
    try:
        payload = sqlite_store.list_runs()
    except Exception as exc:
        logger.warning("Failed to load persisted runs from sqlite: %s", exc)
        return
    now = now_iso()
    with RUN_LOCK:
        for item in payload:
            if not isinstance(item, dict) or "runId" not in item:
                continue
            run = _deserialize_run_record(item)
            if run.status in ("queued", "running"):
                run.status = "failed"
                run.error = "service_restarted"
                run.updatedAt = now
                for step in run.steps:
                    if step.status == "running":
                        step.status = "failed"
                        step.endedAt = now
                RunStore.update_run_status(run.runId, "failed", error="service_restarted", updated_at=now)
            RUNS[run.runId] = run
            CANCEL_FLAGS[run.runId] = threading.Event()


# â”€â”€ Auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def require_auth(x_execution_token: str | None) -> None:
    if x_execution_token != EXECUTION_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# â”€â”€ Event / message / log helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def append_event(run: RunRecord, event_type: str, payload: dict[str, Any]) -> None:
    event = {"type": event_type, "payload": payload, "timestamp": now_iso()}
    with RUN_LOCK:
        run.events.append(event)
        run.updatedAt = event["timestamp"]
    try:
        sqlite_store.append_run_event(run.runId, event_type, payload, event["timestamp"])
    except Exception as exc:
        logger.warning("Failed to append event for run %s: %s", run.runId, exc)
    events_file = Path(run.outputDir) / "events.jsonl"
    events_file.parent.mkdir(parents=True, exist_ok=True)
    with events_file.open("a", encoding="utf-8", newline="") as handle:
        handle.write(json.dumps(event) + "\n")


def emit_chat_delta(run: RunRecord, message_id: str, token: str) -> None:
    """Emit a lightweight streaming token event (no DB persistence)."""
    event = {"type": "chat:delta", "payload": {"messageId": message_id, "token": token}, "timestamp": now_iso()}
    with RUN_LOCK:
        run.events.append(event)


def append_terminal_output(
    run: RunRecord,
    text: str,
    *,
    is_progress: bool = False,
    step_id: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "text": str(text),
        "isProgress": is_progress,
    }
    if step_id in STEP_LABELS:
        payload["stepId"] = step_id
        payload["stepLabel"] = STEP_LABELS[step_id]
    append_event(run, "terminal:output", payload)


def send_terminal_data(run: RunRecord, raw_chunk: str) -> None:
    """Emit a raw PTY chunk to the frontend terminal.

    Unlike ``append_terminal_output`` which sends parsed/cleaned lines,
    this streams the *exact* bytes from the PTY (including ANSI codes and
    control characters) so xterm can render them natively â€” identical to
    bolt.new's WebSocket ``terminal.write(event.data)`` pattern.
    """
    cleaned = raw_chunk.replace("\x00", "")
    if not cleaned:
        return
    append_event(run, "terminal:data", {"data": cleaned})


def _strip_log_tags(message: str) -> str:
    return re.sub(r"^\s*(?:\[[^\]]+\]\s*)+", "", message).strip()


def _clean_terminal_output(message: str) -> str:
    ansi_stripped = re.sub(r"\u001b\[[0-?]*[ -/]*[@-~]", "", message)
    lines: list[str] = []
    for raw_line in ansi_stripped.splitlines():
        line = raw_line
        line = re.sub(r"[Â¿Â´Â³]", " ", line)
        line = re.sub(r"[\u2500-\u257f\u2580-\u259f]", " ", line)
        line = re.sub(r"[\u00c0-\u00ff]", " ", line)
        line = re.sub(r"[=]{3,}", " ", line)
        line = re.sub(r"[?]{5,}", " ", line)
        line = re.sub(r"\s{2,}", " ", line).strip()
        if not line:
            continue
        if re.fullmatch(r"[=\-_*~.#|:+`^]+", line):
            continue
        lines.append(line)
    return "\n".join(lines)


def _sanitize_content(message: str, *, strip_prefix: bool = True) -> str:
    text = str(message)
    if strip_prefix:
        text = _strip_log_tags(text)
    return _clean_terminal_output(text)


def append_chat_message(
    run: RunRecord,
    *,
    role: str,
    kind: str,
    content: str,
    step: dict[str, str] | None = None,
    sql: dict[str, str] | None = None,
    ts: str | None = None,
) -> dict[str, Any]:
    timestamp = ts or now_iso()
    cleaned_content = content if kind == "tool_result" else _sanitize_content(content)
    message: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "ts": timestamp,
        "role": role,
        "kind": kind,
        "content": cleaned_content,
    }

    if step:
        message["step"] = step

    if sql:
        cleaned_sql = {
            key: _sanitize_content(value, strip_prefix=False)
            for key, value in sql.items()
            if isinstance(value, str) and _sanitize_content(value, strip_prefix=False)
        }
        if cleaned_sql:
            message["sql"] = cleaned_sql

    with RUN_LOCK:
        run.messages.append(message)
        run.updatedAt = timestamp
    try:
        sqlite_store.append_run_message(run.runId, message)
    except Exception as exc:
        logger.warning("Failed to append message for run %s: %s", run.runId, exc)
    append_event(run, "chat:message", message)
    return message


def format_activity_log_entry(entry: dict[str, Any]) -> str:
    message = entry.get("message") or ""
    header = str(message).strip()
    data = entry.get("data")
    if not data:
        return header

    def stringify_value(value: Any) -> str:
        if isinstance(value, str):
            return value.rstrip()
        try:
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    if isinstance(data, dict):
        lines: list[str] = []
        for key, value in data.items():
            text = stringify_value(value)
            if not text:
                lines.append(f"{key}:")
            elif "\n" in text:
                lines.append(f"{key}:\n{text}")
            else:
                lines.append(f"{key}: {text}")
        body = "\n".join(lines).rstrip()
        return f"{header}\n{body}" if body else header

    if isinstance(data, str):
        body = data.rstrip()
        return f"{header}\n{body}" if body else header

    body = stringify_value(data)
    return f"{header}\n{body}" if body else header


def update_step(run: RunRecord, step_id: str, status: str) -> None:
    current_time = now_iso()
    started_at = None
    ended_at = None
    with RUN_LOCK:
        for step in run.steps:
            if step.id == step_id:
                step.status = status
                if status == "running":
                    step.startedAt = current_time
                    started_at = current_time
                if status in ("completed", "failed"):
                    step.endedAt = current_time
                    ended_at = current_time
                run.updatedAt = current_time
                break
    try:
        sqlite_store.update_run_step(run.runId, step_id, status, started_at, ended_at)
    except Exception as exc:
        logger.warning("Failed to update step %s for run %s: %s", step_id, run.runId, exc)


def add_log(
    run: RunRecord,
    message: str,
    step_id: str | None = None,
    is_progress: bool = False,
) -> None:
    line = _sanitize_content(str(message), strip_prefix=False).strip()
    if not line:
        return
    created_at = now_iso()
    resolved_step_id = step_id if step_id in STEP_LABELS else None

    if is_progress:
        append_terminal_output(run, line, is_progress=True, step_id=resolved_step_id)
        return

    with RUN_LOCK:
        run.logs.append(line)
        run.updatedAt = created_at
    try:
        sqlite_store.append_run_log(run.runId, line, created_at)
    except Exception as exc:
        logger.warning("Failed to append log for run %s: %s", run.runId, exc)
    append_chat_message(
        run,
        role="system",
        kind="log",
        content=line,
    )


def set_run_status(run: RunRecord, status: str, error: str | None = None) -> None:
    ts = now_iso()
    with RUN_LOCK:
        run.status = status
        run.error = error
        run.updatedAt = ts
    RunStore.update_run_status(run.runId, status, error=error, updated_at=ts)


def get_steps_template() -> list[RunStep]:
    return [RunStep(id=step_id, label=label) for step_id, label in STEP_LABELS.items()]


def ensure_not_canceled(run_id: str) -> None:
    cancel_flag = CANCEL_FLAGS.get(run_id)
    if cancel_flag and cancel_flag.is_set():
        raise RuntimeError("Run canceled")


# â”€â”€ Run-record factory helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _sanitize_upload_filename(name: str) -> str:
    base = Path(name or "uploaded.ddl.sql").name
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in base)
    return safe or "uploaded.ddl.sql"


def _request_from_run(existing: RunRecord) -> StartRunRequest:
    return StartRunRequest(
        projectId=existing.projectId,
        projectName=existing.projectName,
        sourceId=existing.sourceId,
        schemaId=existing.schemaId,
        sourceLanguage=existing.sourceLanguage,
        sourcePath=existing.sourcePath,
        schemaPath=existing.schemaPath,
        sfAccount=existing.sfAccount,
        sfUser=existing.sfUser,
        sfRole=existing.sfRole,
        sfWarehouse=existing.sfWarehouse,
        sfDatabase=existing.sfDatabase,
        sfSchema=existing.sfSchema,
        sfAuthenticator=existing.sfAuthenticator,
    )


# â”€â”€ User message queue helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def push_user_message(run_id: str, message: str) -> None:
    """Queue a user message for the running agent to pick up."""
    with RUN_LOCK:
        if run_id not in USER_MESSAGE_QUEUES:
            USER_MESSAGE_QUEUES[run_id] = []
        USER_MESSAGE_QUEUES[run_id].append(message)
        run = RUNS.get(run_id)
        if run:
            run.userMessageQueue.append(message)


def pop_user_message(run_id: str) -> str | None:
    """Pop the next user message from the queue for a run."""
    with RUN_LOCK:
        queue = USER_MESSAGE_QUEUES.get(run_id, [])
        if queue:
            msg = queue.pop(0)
            run = RUNS.get(run_id)
            if run and run.userMessageQueue:
                run.userMessageQueue.pop(0)
            return msg
    return None

