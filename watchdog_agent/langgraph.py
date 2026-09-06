"""LangGraph's public RunnableConfig callback interface, sync and async."""

from __future__ import annotations

from typing import Any
from uuid import UUID

try:
    from langchain_core.callbacks import BaseCallbackHandler
    from langchain_core.messages import ToolMessage
    from langchain_core.outputs import LLMResult
except ImportError as error:
    raise ImportError("Install the LangGraph adapter: pip install 'watchdog-agent-sdk[langgraph]'") from error

from ._adapter import AdapterEvents
from .client import Run


class WatchdogCallbackHandler(BaseCallbackHandler):
    """Pass in config['callbacks']; the outer Run owns the job lifecycle."""

    run_inline = True

    def __init__(self, run: Run) -> None:
        self.run = run
        self._events = AdapterEvents(run, "langgraph")

    def on_llm_start(self, serialized: dict[str, Any], prompts: list[str], *, run_id: UUID, **kwargs: Any) -> None:
        metadata = kwargs.get("metadata") or {}
        self._events.start("model", run_id, metadata.get("ls_model_name"))

    def on_chat_model_start(self, serialized: dict[str, Any], messages: Any, *, run_id: UUID, **kwargs: Any) -> None:
        self.on_llm_start(serialized, [], run_id=run_id, **kwargs)

    def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        try:
            # First candidate per prompt: n-best generations may repeat usage totals.
            usage = [
                getattr(getattr(group[0], "message", None), "usage_metadata", None)
                for group in response.generations
                if group
            ]
            reported = [item for item in usage if item is not None]
            output = response.llm_output or {}
            tokens = None
            if reported:
                tokens = {
                    field: sum(item.get(field, 0) for item in reported) for field in ("input_tokens", "output_tokens")
                }
            elif isinstance(output.get("token_usage"), dict):
                raw = output["token_usage"]
                tokens = {"input_tokens": raw.get("prompt_tokens", 0), "output_tokens": raw.get("completion_tokens", 0)}
            self._events.model_end(run_id, usage=tokens, model=output.get("model_name"))
        except Exception:
            self.run.client._count("dropped")

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._events.model_end(run_id, error=error)

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        inputs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._events.start(
            "tool", run_id, (serialized or {}).get("name", "tool"), inputs if inputs is not None else input_str
        )

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        self._events.tool_end(run_id, failed=isinstance(output, ToolMessage) and output.status == "error")

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._events.tool_end(run_id, error=error)
