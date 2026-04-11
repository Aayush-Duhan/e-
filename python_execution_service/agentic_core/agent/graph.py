"""LangGraph-based autonomous agent.

Uses a StateGraph with two nodes (call_model, call_tools).

Flow:
    START -> call_model -> [should_continue] -> call_tools -> call_model -> ...
                        -> END (if no tool calls / pause / max iterations)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from typing import Any, Annotated, Callable, Optional, TypedDict

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import StateGraph, START, END

from python_execution_service.agentic_core.agent.context_logger import (
    start_log,
    close_log,
    log_iteration_start,
    log_llm_request,
    log_llm_response,
    log_llm_error,
    log_parsed_action,
    log_tool_start,
    log_tool_result,
    log_user_message,
    log_stopping,
)
from python_execution_service.agentic_core.agent.cortex_chat import (
    get_cortex_client,
    stream_cortex_complete,
)
from python_execution_service.agentic_core.agent.tools import (
    TOOL_MAP,
    TOOL_SCHEMAS,
    get_active_context,
    get_openai_tools,
    set_active_context,
    set_step_callback,
)
from python_execution_service.domain.runs.service import pop_user_message
from python_execution_service.agentic_core.models.context import MigrationContext, MigrationState

logger = logging.getLogger(__name__)

MAX_AGENT_ITERATIONS = 30
MAX_TOOL_RESULT_CHARS = 12000
MAX_CONVERSATION_MESSAGES = 20
MAX_CONVERSATION_CHARS = 100_000

TOOL_RESULT_DISPLAY = {
    "view_file",
    "edit_file",
    "edit_file_batch",
    "get_converted_file_info",
    "list_files",
    "search_file",
    "read_file",
    "write_file",
    "make_directory",
    "execute_sql_range",
}

# ── System prompt ──────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an autonomous Snowflake migration agent. You execute migration steps \
using tools and communicate with the user.

## Available Tools

You have access to the following tools via function calling. The system will \
automatically provide the tool schemas — call them by name with the required arguments.

### Pipeline Tools
These accept only `session_id` (value: {session_id}):
- **init_project** — Initialize the SCAI project (always first)
- **add_source_code** — Ingest source SQL files
- **apply_schema_mapping** — Apply schema mapping CSV (skips if no CSV)
- **convert_code** — Convert source SQL to Snowflake SQL
- **execute_sql** — Execute converted SQL on Snowflake
- **validate_output** — Validate the conversion quality
- **finalize_migration** — Generate final report (only after success)

### File Tools
These accept `session_id` plus additional arguments:
- **get_converted_file_info** — Get metadata (paths, lines, size)
- **view_file** — View a section of a file with line numbers
- **edit_file** — Replace a range of lines with new content
- **edit_file_batch** — Apply multiple line edits in one call
- **list_files** — List files/directories under project root
- **search_file** — Search within a file
- **read_file** — Read file contents
- **write_file** — Write full file contents
- **make_directory** — Create a directory
- **execute_sql_range** — Execute SQL from a specific line range

## Important Notes

- Always pass `session_id` = "{session_id}" to every tool call.
- Call ONE tool at a time. Wait for results before deciding next action.
- If you want to communicate without calling a tool, just write your message.
- When you are done or need user action, simply respond with a text message \
explaining the situation — do NOT call any tool. The system will automatically \
stop the loop when you respond without tool calls.

## Execution Strategy

1. Start with init_project → add_source_code → apply_schema_mapping → convert_code → execute_sql
2. After execute_sql:
   - Success → validate_output → finalize_migration
   - Errors (NOT missing objects) → use get_converted_file_info, view_file, \
and edit_file to diagnose and fix, then execute_sql again (retry up to 5 times)
   - Missing objects / DDL needed → tell the user what is missing and stop.
3. After validate_output:
   - Passed → finalize_migration
   - Failed → use view_file + edit_file to fix, then execute_sql again

## Error Recovery Strategy

When you encounter execution or validation errors:
1. Call get_converted_file_info to see the file paths and sizes
2. Use the error message to identify the problematic area
3. Call view_file to examine the relevant section
4. Call edit_file to apply a targeted fix to ONLY the affected lines
5. After editing, call execute_sql to retry
6. If the same error persists after 3 attempts, explain the issue to the user and stop.

## Response Format

All human-readable text must be in GitHub-flavored Markdown.
Use short headings, bullets, and fenced code blocks for SQL/JSON/logs.

## Project Info
Source language: {source_language}
Project name: {project_name}
Has schema mapping: {has_schema_mapping}
"""

# ── Follow-up status summary ───────────────────────────────────

def _build_status_summary(context: MigrationContext) -> str:
    """Build a compact status summary from MigrationContext for follow-up runs.

    This replaces replaying the full prior tool transcript. The LLM gets
    everything it needs to continue from context state alone.
    """
    parts: list[str] = []
    parts.append(f"**Current stage**: {context.current_stage.value}")

    if context.converted_files:
        parts.append(f"**Converted files**: {', '.join(context.converted_files[-5:])}")

    if context.execution_passed is not None:
        parts.append(f"**Execution passed**: {context.execution_passed}")

    if context.execution_errors:
        recent = context.execution_errors[-3:]
        err_lines = [f"- {e.get('type', 'error')}: {str(e.get('message', ''))[:200]}" for e in recent]
        parts.append("**Recent execution errors**:\n" + "\n".join(err_lines))

    if context.missing_objects:
        parts.append(f"**Missing objects**: {', '.join(context.missing_objects[:10])}")

    if context.validation_passed is not None:
        parts.append(f"**Validation passed**: {context.validation_passed}")

    if context.validation_issues:
        recent = context.validation_issues[-3:]
        issue_lines = [
            f"- {i.get('severity', '?')}: {str(i.get('message', ''))[:200]}"
            for i in recent
        ]
        parts.append("**Recent validation issues**:\n" + "\n".join(issue_lines))

    if context.self_heal_iteration > 0:
        parts.append(
            f"**Self-heal iteration**: {context.self_heal_iteration}/{context.max_self_heal_iterations}"
        )

    if context.requires_ddl_upload:
        parts.append(f"**Requires DDL upload**: {context.human_intervention_reason or 'Yes'}")

    if context.last_executed_file_index >= 0:
        parts.append(f"**Last executed file index**: {context.last_executed_file_index}")

    if context.errors:
        parts.append(f"**Last error**: {context.errors[-1][:300]}")

    return (
        "## Migration Status (continued from previous run)\n\n"
        + "\n".join(parts)
        + "\n\nThe user is sending a follow-up message. Continue the migration "
        "from the current state. All tools and project files are still available."
    )


# ── Conversation truncation ────────────────────────────────────

def _truncate_conversation(
    messages: list[dict],
    max_messages: int = MAX_CONVERSATION_MESSAGES,
    max_chars: int = MAX_CONVERSATION_CHARS,
) -> list[dict]:
    """Truncate conversation history to stay within safe limits.

    Always keeps the first message (system prompt) and the most recent messages.
    Inserts a note when messages are dropped so the LLM knows context was trimmed.
    """
    if len(messages) <= max_messages + 1:
        total_chars = sum(len(str(m.get("content", "") or "")) for m in messages)
        if total_chars <= max_chars:
            return messages

    # Always keep the system prompt (first message)
    system_msg = messages[0] if messages and messages[0].get("role") == "system" else None
    rest = messages[1:] if system_msg else messages

    # Trim to max_messages from the end
    if len(rest) > max_messages:
        rest = rest[-max_messages:]

    # Further trim if total chars still exceeds limit
    while len(rest) > 2:
        total_chars = sum(len(str(m.get("content", "") or "")) for m in rest)
        if system_msg:
            total_chars += len(str(system_msg.get("content", "") or ""))
        if total_chars <= max_chars:
            break
        rest = rest[1:]

    truncation_note = {
        "role": "system",
        "content": (
            "[Earlier conversation history was truncated to stay within context limits. "
            "The most recent messages are preserved above.]"
        ),
    }

    result = []
    if system_msg:
        result.append(system_msg)
    result.append(truncation_note)
    result.extend(rest)
    return result


# ── Tool dispatch ──────────────────────────────────────────────



def _format_tool_result_for_chat(tool_name: str, tool_result: str) -> str:
    try:
        data = json.loads(tool_result)
    except Exception:
        data = {"tool": tool_name, "raw": tool_result}

    if isinstance(data, dict) and "tool" not in data:
        data["tool"] = tool_name

    payload = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    if len(payload) <= MAX_TOOL_RESULT_CHARS:
        return payload

    wrapper = {
        "tool": tool_name,
        "truncated": True,
        "total_chars": len(payload),
        "preview": payload[:MAX_TOOL_RESULT_CHARS],
    }
    return json.dumps(wrapper, indent=2, ensure_ascii=False, default=str)


def execute_tool(tool_name: str, session_id: str, extra_args: dict | None = None) -> str:
    tool_fn = TOOL_MAP.get(tool_name)
    if tool_fn is None:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})

    try:
        kwargs = {"session_id": session_id}
        if extra_args:
            kwargs.update(extra_args)
        result = tool_fn(**kwargs)
        return result if isinstance(result, str) else json.dumps(result, default=str)
    except Exception as exc:
        return json.dumps({"error": f"Tool {tool_name} failed: {exc}"})


# ── LangGraph state ───────────────────────────────────────────

def _add_messages(existing: list[dict], new: list[dict]) -> list[dict]:
    return existing + new


class AgentState(TypedDict):
    messages: Annotated[list[dict], _add_messages]
    session_id: str
    iteration: int
    is_done: bool


# ── Callback holders (set per-run before graph invocation) ─────

_MESSAGE_CALLBACKS: dict[str, Callable] = {}
_STEP_CALLBACKS: dict[str, Callable] = {}
_USER_MESSAGE_GETTERS: dict[str, Callable] = {}
_CONVERSATION_CALLBACKS: dict[str, Callable] = {}
_SF_SESSIONS: dict[str, Any] = {}
_CORTEX_CLIENTS: dict[str, Any] = {}
_DELTA_CALLBACKS: dict[str, Callable] = {}


def set_run_callbacks(
    session_id: str,
    *,
    message_callback: Optional[Callable] = None,
    step_callback: Optional[Callable] = None,
    user_message_getter: Optional[Callable] = None,
    conversation_callback: Optional[Callable] = None,
    delta_callback: Optional[Callable] = None,
    sf_session: Any = None,
    cortex_client: Any = None,
) -> None:
    if message_callback:
        _MESSAGE_CALLBACKS[session_id] = message_callback
    if step_callback:
        _STEP_CALLBACKS[session_id] = step_callback
    if user_message_getter:
        _USER_MESSAGE_GETTERS[session_id] = user_message_getter
    if conversation_callback:
        _CONVERSATION_CALLBACKS[session_id] = conversation_callback
    if delta_callback:
        _DELTA_CALLBACKS[session_id] = delta_callback
    if sf_session:
        _SF_SESSIONS[session_id] = sf_session
    if cortex_client:
        _CORTEX_CLIENTS[session_id] = cortex_client


def cleanup_run_callbacks(session_id: str) -> None:
    _MESSAGE_CALLBACKS.pop(session_id, None)
    _STEP_CALLBACKS.pop(session_id, None)
    _USER_MESSAGE_GETTERS.pop(session_id, None)
    _CONVERSATION_CALLBACKS.pop(session_id, None)
    _DELTA_CALLBACKS.pop(session_id, None)
    _CORTEX_CLIENTS.pop(session_id, None)
    sf = _SF_SESSIONS.pop(session_id, None)
    if sf:
        try:
            sf.close()
        except Exception:
            pass


def _emit(session_id: str, role: str, kind: str, content: str) -> None:
    cb = _MESSAGE_CALLBACKS.get(session_id)
    if cb and content.strip():
        try:
            cb(role, kind, content)
        except Exception:
            pass


def _emit_delta(session_id: str, message_id: str, token: str) -> None:
    cb = _DELTA_CALLBACKS.get(session_id)
    if cb:
        try:
            cb(message_id, token)
        except Exception:
            pass


def _sync_conversation(session_id: str, messages: list[dict]) -> None:
    cb = _CONVERSATION_CALLBACKS.get(session_id)
    if not cb:
        return
    try:
        serializable: list[dict] = []
        for m in messages:
            entry: dict[str, Any] = {
                "role": str(m.get("role", "user")),
            }
            # Preserve None content as None (not the string "None")
            raw_content = m.get("content")
            entry["content"] = str(raw_content) if raw_content is not None else None
            # Preserve tool_calls on assistant messages
            if m.get("tool_calls"):
                entry["tool_calls"] = m["tool_calls"]
            # Preserve tool_call_id on tool messages
            if m.get("tool_call_id"):
                entry["tool_call_id"] = m["tool_call_id"]
            serializable.append(entry)
        cb(serializable)
    except Exception:
        pass


# ── Graph nodes ────────────────────────────────────────────────

def call_model(state: AgentState) -> dict:
    session_id = state["session_id"]
    iteration = state["iteration"] + 1

    log_iteration_start(session_id, iteration)

    # Check for pending user messages
    getter = _USER_MESSAGE_GETTERS.get(session_id)
    new_messages: list[dict] = []
    if getter and iteration > 1:
        user_msg = getter()
        if user_msg and user_msg.strip():
            log_user_message(session_id, user_msg)
            new_messages.append({"role": "user", "content": user_msg})

    # Build full conversation for the LLM call
    all_messages = state["messages"] + new_messages

    # Safety net: truncate if accumulated messages exceed context limits
    total_chars = sum(len(str(m.get("content", "") or "")) for m in all_messages)
    if total_chars > MAX_CONVERSATION_CHARS or len(all_messages) > MAX_CONVERSATION_MESSAGES * 2:
        all_messages = _truncate_conversation(all_messages)

    log_llm_request(session_id, len(all_messages))

    cortex_client = _CORTEX_CLIENTS.get(session_id)
    if not cortex_client:
        error_msg = "No Cortex client available. Check SNOWFLAKE_PAT and SNOWFLAKE_ACCOUNT_URL."
        log_llm_error(session_id, error_msg)
        _emit(session_id, "error", "run_status", error_msg)
        return {"messages": new_messages, "iteration": iteration, "is_done": True}

    openai_tools = get_openai_tools()

    # Stream token-by-token from Cortex
    msg_id = str(uuid.uuid4())
    content_parts: list[str] = []
    tool_calls: list[dict] | None = None
    finish_reason = "stop"

    try:
        _emit_delta(session_id, msg_id, "")

        for event in stream_cortex_complete(
            cortex_client,
            all_messages,
            tools=openai_tools,
            max_tokens=4096,
        ):
            if event["type"] == "content_delta":
                content_parts.append(event["content"])
                _emit_delta(session_id, msg_id, event["content"])
            elif event["type"] == "tool_calls_complete":
                tool_calls = event["tool_calls"]
            elif event["type"] == "done":
                finish_reason = event.get("finish_reason", "stop")
    except Exception as exc:
        error_msg = f"Agent LLM call failed: {exc}"
        logger.error("Agent LLM call failed: %s", exc, exc_info=True)
        log_llm_error(session_id, error_msg)
        _emit(session_id, "error", "run_status", error_msg)
        log_stopping(session_id, error_msg)
        return {"messages": new_messages, "iteration": iteration, "is_done": True}

    content = "".join(content_parts) if content_parts else ""

    log_llm_response(session_id, content or json.dumps(tool_calls or [], default=str))

    assistant_msg: dict[str, Any] = {"role": "assistant"}
    if content:
        assistant_msg["content"] = content
    if tool_calls:
        assistant_msg["tool_calls"] = tool_calls
        if not content:
            assistant_msg["content"] = None

    new_messages.append(assistant_msg)

    if not tool_calls and content:
        _emit(session_id, "agent", "agent_response", content)
        log_parsed_action(session_id, None, content, {})

    _sync_conversation(session_id, state["messages"] + new_messages)

    return {"messages": new_messages, "iteration": iteration}


def call_tools(state: AgentState) -> dict:
    session_id = state["session_id"]
    messages = state["messages"]

    # Find the last assistant message with tool_calls
    last_assistant = None
    for msg in reversed(messages):
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            last_assistant = msg
            break

    if not last_assistant:
        return {"messages": [], "is_done": False}

    tool_calls = last_assistant.get("tool_calls", [])
    content = last_assistant.get("content") or ""
    new_messages: list[dict] = []

    for tc in tool_calls:
        tc_id = tc.get("id", "")
        func = tc.get("function", {})
        tool_name = func.get("name", "")
        arguments_str = func.get("arguments", "{}")

        try:
            extra_args = json.loads(arguments_str) if arguments_str else {}
        except json.JSONDecodeError:
            extra_args = {}

        log_parsed_action(session_id, tool_name, content or "", extra_args)

        if content:
            _emit(session_id, "agent", "thinking", content)
            content = ""

        # Execute the tool
        logger.info("Agent calling tool: %s", tool_name)
        _emit(session_id, "system", "step_started", f"Executing: {tool_name}")
        log_tool_start(session_id, tool_name)

        cb = _STEP_CALLBACKS.get(session_id)
        if cb:
            cb(tool_name, "running")

        tool_result = execute_tool(
            tool_name,
            extra_args.pop("session_id", session_id),
            extra_args,
        )

        try:
            result_data = json.loads(tool_result)
            success = result_data.get("success", False)
            summary = result_data.get("summary", "")
        except (json.JSONDecodeError, TypeError):
            success = True
            summary = tool_result[:200]

        log_tool_result(session_id, tool_name, tool_result, success, summary)

        if cb:
            cb(tool_name, "completed" if success else "failed")

        if tool_name in TOOL_RESULT_DISPLAY:
            tool_payload = _format_tool_result_for_chat(tool_name, tool_result)
            _emit(session_id, "agent", "tool_result", tool_payload)

        new_messages.append({
            "role": "tool",
            "tool_call_id": tc_id,
            "content": tool_result,
        })
        _sync_conversation(session_id, state["messages"] + new_messages)

    return {"messages": new_messages, "is_done": False}


def should_continue(state: AgentState) -> str:
    session_id = state.get("session_id", "?")
    if state["is_done"]:
        return END

    if state["iteration"] >= MAX_AGENT_ITERATIONS:
        _emit(
            session_id,
            "agent",
            "agent_response",
            "I've reached the maximum number of steps for this run. "
            "You can send a follow-up message to continue where I left off.",
        )
        log_stopping(state["session_id"], "Max iterations reached")
        return END

    messages = state["messages"]
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            if msg.get("tool_calls"):
                return "call_tools"
            return END
        break

    return END


# ── Graph builder ──────────────────────────────────────────────

def build_graph(checkpointer=None):
    graph = StateGraph(AgentState)
    graph.add_node("call_model", call_model)
    graph.add_node("call_tools", call_tools)
    graph.add_edge(START, "call_model")
    graph.add_conditional_edges("call_model", should_continue, {"call_tools": "call_tools", END: END})
    graph.add_edge("call_tools", "call_model")
    return graph.compile(checkpointer=checkpointer)


def create_checkpointer(db_path: str) -> SqliteSaver:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return SqliteSaver(conn)


# ── High-level API for workflow.py ─────────────────────────────

def build_agent_graph(
    context: MigrationContext,
    *,
    message_callback: Optional[Callable[[str, str, str], None]] = None,
    step_callback: Optional[Callable[[str, str], None]] = None,
    user_message_getter: Optional[Callable[[], Optional[str]]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
    conversation_callback: Optional[Callable[[list[dict[str, str]]], None]] = None,
    delta_callback: Optional[Callable[[str, str], None]] = None,
    consume_user_messages_from_start: bool = False,
    start_with_migration_prompt: bool = True,
    checkpointer=None,
) -> dict:
    """Build the agent graph and return config needed by workflow.py.

    Returns a dict with:
        - "graph": compiled StateGraph
        - "initial_state": initial AgentState to invoke with
        - "config": LangGraph config dict (thread_id, etc.)
        - "_context": MigrationContext reference
    """
    session_id = context.session_id
    set_active_context(session_id, context)
    set_step_callback(session_id, step_callback)

    log_file = start_log(
        session_id,
        context.project_path or os.path.join(os.getcwd(), "agent_logs"),
        project_name=context.project_name or "unknown",
        source_language=context.source_language or "teradata",
    )
    logger.info("Agent context log: %s", log_file)

    cortex_client = get_cortex_client()

    set_run_callbacks(
        session_id,
        message_callback=message_callback,
        step_callback=step_callback,
        user_message_getter=user_message_getter if not consume_user_messages_from_start else user_message_getter,
        conversation_callback=conversation_callback,
        delta_callback=delta_callback,
        cortex_client=cortex_client,
    )

    system_prompt = SYSTEM_PROMPT.format(
        session_id=session_id,
        source_language=context.source_language or "teradata",
        project_name=context.project_name or "migration_project",
        has_schema_mapping="Yes" if context.mapping_csv_path else "No",
    )

    if conversation_history:
        messages: list[dict] = []
        for m in conversation_history:
            if not isinstance(m, dict):
                continue
            entry: dict[str, Any] = {"role": str(m.get("role", "user"))}
            raw_content = m.get("content")
            entry["content"] = str(raw_content) if raw_content is not None else None
            if m.get("tool_calls"):
                entry["tool_calls"] = m["tool_calls"]
            if m.get("tool_call_id"):
                entry["tool_call_id"] = m["tool_call_id"]
            messages.append(entry)
        if messages and messages[0]["role"] == "system":
            messages[0]["content"] = system_prompt
        else:
            messages.insert(0, {"role": "system", "content": system_prompt})
        # Truncate to avoid exceeding model context window on follow-up runs
        messages = _truncate_conversation(messages)
    else:
        messages = [{"role": "system", "content": system_prompt}]
        if start_with_migration_prompt:
            messages.append({
                "role": "user",
                "content": "Begin the migration process. Execute the tools in order.",
            })

    # For follow-up runs, drain any queued user messages.
    drained_user_messages: list[dict] = []
    if consume_user_messages_from_start:
        while True:
            queued = pop_user_message(session_id)
            if not queued:
                break
            drained_user_messages.append({"role": "user", "content": queued})

    # ── Follow-up: clean fresh start ──────────────────────────────
    # Instead of replaying prior tool transcript (which causes malformed
    # assistant/tool pairing errors with Cortex), build a minimal clean
    # conversation: system prompt + status summary + user follow-up.
    # All execution state is preserved in MigrationContext, not in the
    # LLM message history.
    if consume_user_messages_from_start:
        status_summary = _build_status_summary(context)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "assistant", "content": status_summary},
        ]
        if drained_user_messages:
            messages.extend(drained_user_messages)
        else:
            messages.append({
                "role": "user",
                "content": "Continue from the current migration state.",
            })
    elif not conversation_history:
        # Initial run with no prior history — messages already set above
        pass

    # For follow-up runs, compile graph WITHOUT the checkpointer so old
    # tool-laden checkpoint state is not merged into the fresh conversation.
    # The checkpointer is only useful for initial runs.
    if consume_user_messages_from_start:
        compiled_graph = build_graph(checkpointer=None)
    else:
        compiled_graph = build_graph(checkpointer=checkpointer)

    initial_state = {
        "messages": messages,
        "session_id": session_id,
        "iteration": 0,
        "is_done": False,
    }

    config = {"configurable": {"thread_id": session_id}}

    return {
        "graph": compiled_graph,
        "initial_state": initial_state,
        "config": config,
        "_context": context,
    }


def cleanup_agent_session(graph_info: Any) -> None:
    if isinstance(graph_info, dict):
        ctx = graph_info.get("_context")
        if ctx and hasattr(ctx, "session_id"):
            close_log(ctx.session_id)
            cleanup_run_callbacks(ctx.session_id)
