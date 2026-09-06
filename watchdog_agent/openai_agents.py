"""OpenAI Agents lifecycle adapter: metadata only, no prompt or output capture.

Create one hook instance per Watchdog Run. The surrounding run context owns
completion/failure, so nested agents and handoffs cannot prematurely finish a job.
"""

from __future__ import annotations

from collections import defaultdict, deque
import time
from typing import Any

try:
    from agents import RunHooks
except ImportError as error:
    raise ImportError("From the Watchdog repository root, install: python -m pip install './sdk[openai]'") from error

from .client import Run


class WatchdogHooks(RunHooks):
    def __init__(self, run: Run) -> None:
        self.run = run
        self._tools: dict[tuple[str, str], deque[tuple[float, str | None]]] = defaultdict(deque)

    async def on_agent_start(self, context: Any, agent: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()

    async def on_agent_end(self, context: Any, agent: Any, output: Any, **kwargs: Any) -> None:
        # A sub-agent ending is not the parent job completing; never inspect output.
        pass

    async def on_handoff(self, context: Any, from_agent: Any, to_agent: Any, **kwargs: Any) -> None:
        # Repeated handoffs are activity, not evidence of business progress.
        self.run.check_cancelled()

    async def on_llm_start(self, context: Any, agent: Any, system_prompt: Any, input_items: Any, **kwargs: Any) -> None:
        # Prompt/input values are deliberately ignored.
        self.run.check_cancelled()

    async def on_llm_end(self, context: Any, agent: Any, response: Any, **kwargs: Any) -> None:
        try:
            usage = getattr(response, "usage", None)
            if usage is None:
                return
            model = getattr(agent, "model", None)
            if not isinstance(model, str):
                model = getattr(model, "model", None)
            self.run.usage(
                model=model if isinstance(model, str) else None,
                input_tokens=getattr(usage, "input_tokens", 0),
                output_tokens=getattr(usage, "output_tokens", 0),
            )
        except Exception:
            self.run.client._count("dropped")

    async def on_tool_start(self, context: Any, agent: Any, tool: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()
        try:
            name = getattr(tool, "name", "tool")
            if not isinstance(name, str):
                name = "tool"
            call_id = getattr(context, "tool_call_id", "")
            if not isinstance(call_id, str):
                call_id = ""
            arguments = getattr(context, "tool_arguments", None)
            key = (call_id, name)
            # Bound leaked starts when a tool raises and the framework skips on_tool_end.
            if len(self._tools) >= 1024 and key not in self._tools:
                self._tools.pop(next(iter(self._tools)))
            entries = self._tools[key]
            if len(entries) < 1024:
                entries.append((time.monotonic(), self.run.fingerprint(name, arguments)))
        except Exception:
            self.run.client._count("dropped")

    async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: Any, **kwargs: Any) -> None:
        try:
            name = getattr(tool, "name", "tool")
            if not isinstance(name, str):
                name = "tool"
            call_id = getattr(context, "tool_call_id", "")
            if not isinstance(call_id, str):
                call_id = ""
            key = (call_id, name)
            entries = self._tools.get(key)
            started, fingerprint = entries.popleft() if entries else (time.monotonic(), None)
            if not entries:
                self._tools.pop(key, None)
            # A string tool result is not a reliable error flag, and may contain secrets.
            # Tool exceptions which are converted to strings need explicit instrumentation
            # in the customer tool implementation for dependency-error classification.
            self.run.tool(name, duration_ms=max(0, (time.monotonic() - started) * 1000), fingerprint=fingerprint)
        except Exception:
            self.run.client._count("dropped")
