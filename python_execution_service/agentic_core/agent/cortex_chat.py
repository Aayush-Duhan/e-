"""Snowflake Cortex REST API client via the OpenAI Python SDK.

Uses the Chat Completions endpoint:
    POST https://{account}.snowflakecomputing.com/api/v2/cortex/v1/chat/completions

Authentication is via a Snowflake Programmatic Access Token (PAT).
The Snowpark session is NOT required for LLM calls — only for SQL execution.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterator

from openai import OpenAI

from python_execution_service.app.config.settings import (
    SNOWFLAKE_ACCOUNT_URL,
    SNOWFLAKE_PAT,
)

logger = logging.getLogger(__name__)

DEFAULT_AGENT_MODEL = "claude-4-sonnet"


def get_agent_model_name() -> str:
    """Resolve the model name for the agent orchestrator."""
    return (os.getenv("CORTEX_MODEL") or DEFAULT_AGENT_MODEL).strip() or DEFAULT_AGENT_MODEL


def get_cortex_client() -> OpenAI:
    """Create an OpenAI SDK client pointing at Snowflake Cortex.

    Requires env vars:
        SNOWFLAKE_PAT          — Programmatic Access Token
        SNOWFLAKE_ACCOUNT_URL  — e.g. https://<account>.snowflakecomputing.com
    """
    pat = SNOWFLAKE_PAT
    account_url = SNOWFLAKE_ACCOUNT_URL

    if not pat:
        raise RuntimeError(
            "SNOWFLAKE_PAT environment variable is required for Cortex REST API. "
            "Generate a Programmatic Access Token in Snowflake."
        )
    if not account_url:
        raise RuntimeError(
            "SNOWFLAKE_ACCOUNT_URL environment variable is required. "
            "Set it to https://<account-identifier>.snowflakecomputing.com"
        )

    base_url = f"{account_url.rstrip('/')}/api/v2/cortex/v1"

    return OpenAI(api_key=pat, base_url=base_url)


# ---------------------------------------------------------------------------
# Public API — Streaming
# ---------------------------------------------------------------------------

def stream_cortex_complete(
    client: OpenAI,
    messages: list[dict[str, str]],
    *,
    tools: list[dict] | None = None,
    tool_choice: str | dict = "auto",
    model: str | None = None,
    temperature: float = 0,
    max_tokens: int = 4096,
    top_p: float | None = None,
) -> Iterator[dict]:
    """Stream from the Cortex REST API via the OpenAI SDK.

    Yields event dicts of the following types:
        {"type": "content_delta", "content": str}
        {"type": "tool_call_delta", "index": int, "id": str, "name": str, "arguments": str}
        {"type": "usage", "usage": dict}
        {"type": "tool_calls_complete", "tool_calls": list[dict]}
        {"type": "done", "finish_reason": str}
    """
    model_name = model or get_agent_model_name()

    create_kwargs: dict[str, Any] = {
        "model": model_name,
        "messages": messages,
        "max_completion_tokens": max_tokens,
        "temperature": temperature,
        "stream": True,
        "stream_options": {"include_usage": True},
    }

    if top_p is not None:
        create_kwargs["top_p"] = top_p

    if tools:
        create_kwargs["tools"] = tools
        create_kwargs["tool_choice"] = tool_choice

    tool_calls_acc: dict[int, dict] = {}
    finish_reason = "stop"

    response = client.chat.completions.create(**create_kwargs)

    for chunk in response:
        if not chunk.choices and chunk.usage:
            yield {
                "type": "usage",
                "usage": {
                    "prompt_tokens": chunk.usage.prompt_tokens,
                    "completion_tokens": chunk.usage.completion_tokens,
                    "total_tokens": chunk.usage.total_tokens,
                },
            }
            continue

        if not chunk.choices:
            continue

        choice = chunk.choices[0]
        delta = choice.delta

        if choice.finish_reason:
            finish_reason = choice.finish_reason

        if delta and delta.content:
            yield {"type": "content_delta", "content": delta.content}

        if delta and delta.tool_calls:
            for tc in delta.tool_calls:
                idx = tc.index if tc.index is not None else 0
                if idx not in tool_calls_acc:
                    tool_calls_acc[idx] = {
                        "id": tc.id or "",
                        "type": "function",
                        "function": {
                            "name": (tc.function.name or "") if tc.function else "",
                            "arguments": "",
                        },
                    }
                else:
                    if tc.id:
                        tool_calls_acc[idx]["id"] = tc.id
                    if tc.function and tc.function.name:
                        tool_calls_acc[idx]["function"]["name"] = tc.function.name

                arg_chunk = (tc.function.arguments or "") if tc.function else ""
                if arg_chunk:
                    tool_calls_acc[idx]["function"]["arguments"] += arg_chunk
                    yield {
                        "type": "tool_call_delta",
                        "index": idx,
                        "id": tool_calls_acc[idx]["id"],
                        "name": tool_calls_acc[idx]["function"]["name"],
                        "arguments": arg_chunk,
                    }

    if tool_calls_acc:
        assembled = [tool_calls_acc[i] for i in sorted(tool_calls_acc.keys())]
        yield {"type": "tool_calls_complete", "tool_calls": assembled}

    yield {"type": "done", "finish_reason": finish_reason}
