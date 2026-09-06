"""Observe Claude's official tool hooks and finite query/result streams."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
import sys
from typing import Any

try:
    from claude_agent_sdk import HookMatcher, ResultMessage
    from claude_agent_sdk.types import HookContext, HookEvent, HookInput, HookJSONOutput, Message
except ImportError as error:
    raise ImportError("Install the Claude adapter: pip install 'watchdog-agent-sdk[claude]'") from error

from ._adapter import AdapterEvents
from .client import Run


class WatchdogClaudeMonitor:
    """One monitor per finite query() or client.receive_response() stream.

    Keep the stream inside the outer Run and consume it to exhaustion. Hooks are
    observation-only: they never allow, deny, or modify a tool invocation.
    """

    def __init__(self, run: Run) -> None:
        self.run = run
        self._events = AdapterEvents(run, "claude_agents")

    def hooks(self, existing: dict[HookEvent, list[HookMatcher]] | None = None) -> dict[HookEvent, list[HookMatcher]]:
        """Copy and append to existing hooks, preserving customer permission hooks."""
        merged = {name: list(matchers) for name, matchers in (existing or {}).items()}
        for name in ("PreToolUse", "PostToolUse", "PostToolUseFailure"):
            merged.setdefault(name, []).append(HookMatcher(hooks=[self._tool_hook]))
        return merged

    async def _tool_hook(self, input_data: HookInput, tool_use_id: str | None, context: HookContext) -> HookJSONOutput:
        try:
            data: Any = input_data
            name = data.get("tool_name", "tool")
            key = (data.get("session_id", ""), tool_use_id or data.get("tool_use_id", ""), name)
            event = data.get("hook_event_name")
            if event == "PreToolUse":
                self._events.start("tool", key, name, data.get("tool_input"))
            elif event in ("PostToolUse", "PostToolUseFailure"):
                self._events.tool_end(key, name=name, failed=event == "PostToolUseFailure")
        except Exception:
            self.run.client._count("dropped")
        return {}

    async def stream(self, messages: AsyncIterable[Message]) -> AsyncIterator[Message]:
        """Yield original messages; mark reported errors or incomplete streams failed.

        Per-result usage covers the main agent loop only. Cumulative session cost
        and model_usage are deliberately not added to per-run usage.
        """
        saw_result = False
        source = aiter(messages)
        try:
            async for message in source:
                self.run.check_cancelled()
                if isinstance(message, ResultMessage):
                    saw_result = True
                    try:
                        usage = message.usage
                        if usage is not None:
                            usage = {
                                "input_tokens": sum(
                                    usage.get(field, 0)
                                    for field in (
                                        "input_tokens",
                                        "cache_read_input_tokens",
                                        "cache_creation_input_tokens",
                                    )
                                ),
                                "output_tokens": usage.get("output_tokens", 0),
                            }
                        self._events.model_end(
                            object(),
                            usage=usage,
                            scope="main_agent_turn",
                            failed=message.is_error or message.subtype != "success",
                        )
                    except Exception:
                        self.run.client._count("dropped")
                    if message.terminal_reason in ("aborted_streaming", "aborted_tools"):
                        self.run._finish("run.cancelled")
                    elif message.is_error or message.subtype != "success":
                        self.run._finish("run.failed", {"metadata": {"error_type": "ClaudeAgentError"}})
                yield message
        finally:
            # Let the outer context record the original application exception.
            # A terminal result already proves the turn ended, even if the caller
            # breaks immediately after it. Closing before a result is incomplete.
            error_type = sys.exc_info()[0]
            if error_type in (None, GeneratorExit) and not saw_result:
                self.run._finish("run.failed", {"metadata": {"error_type": "ClaudeIncompleteStream"}})
            close = getattr(source, "aclose", None)
            if close is not None:
                await close()
