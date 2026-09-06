"""OpenAI Agents RunHooks. Create one observer per outer Watchdog Run."""

from __future__ import annotations

from typing import Any

try:
    from agents import RunHooks
except ImportError as error:
    raise ImportError("Install the OpenAI adapter: pip install 'watchdog-agent-sdk[openai]'") from error

from ._adapter import AdapterEvents
from .client import Run


class WatchdogHooks(RunHooks):
    def __init__(self, run: Run) -> None:
        self.run = run
        self._events = AdapterEvents(run, "openai_agents")

    async def on_agent_start(self, context: Any, agent: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()

    async def on_agent_end(self, context: Any, agent: Any, output: Any, **kwargs: Any) -> None:
        # A nested agent ending must not finish the surrounding application job.
        pass

    async def on_handoff(self, context: Any, from_agent: Any, to_agent: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()

    async def on_llm_start(self, context: Any, agent: Any, system_prompt: Any, input_items: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()
        self._events.start("model", (id(context), id(agent)))

    async def on_llm_end(self, context: Any, agent: Any, response: Any, **kwargs: Any) -> None:
        try:
            model = getattr(agent, "model", None)
            if not isinstance(model, str):
                model = getattr(model, "model", None)
            self._events.model_end((id(context), id(agent)), model=model, usage=getattr(response, "usage", None))
        except Exception:
            self.run.client._count("dropped")

    @staticmethod
    def _tool_key(context: Any, tool: Any) -> tuple[str, str]:
        name = getattr(tool, "name", "tool")
        call_id = getattr(context, "tool_call_id", "")
        return call_id if isinstance(call_id, str) else "", name if isinstance(name, str) else "tool"

    async def on_tool_start(self, context: Any, agent: Any, tool: Any, **kwargs: Any) -> None:
        self.run.check_cancelled()
        try:
            key = self._tool_key(context, tool)
            self._events.start("tool", key, key[1], getattr(context, "tool_arguments", None))
        except Exception:
            self.run.client._count("dropped")

    async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: Any, **kwargs: Any) -> None:
        # RunHooks has no tool-error callback. Do not infer failure from result text.
        try:
            key = self._tool_key(context, tool)
            self._events.tool_end(key, name=key[1])
        except Exception:
            self.run.client._count("dropped")
