"""Native PydanticAI capability; no tracing exporter or model patching."""

from __future__ import annotations

from typing import Any

try:
    from pydantic_ai import RunContext, ToolDefinition
    from pydantic_ai.capabilities import Capability, WrapModelRequestHandler, WrapToolExecuteHandler
    from pydantic_ai.exceptions import ApprovalRequired, CallDeferred, SkipModelRequest, SkipToolExecution
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.models import ModelRequestContext
except ImportError as error:
    raise ImportError("Install the PydanticAI adapter: pip install 'watchdog-agent-sdk[pydanticai]'") from error

from ._adapter import AdapterEvents
from .client import Run


class WatchdogCapability(Capability[Any]):
    """Pass a fresh instance in each agent.run(..., capabilities=[...]) call."""

    def __init__(self, run: Run) -> None:
        super().__init__(id="watchdog")
        self.run = run
        self._events = AdapterEvents(run, "pydantic_ai")

    async def wrap_model_request(
        self, ctx: RunContext[Any], *, request_context: ModelRequestContext, handler: WrapModelRequestHandler
    ) -> ModelResponse:
        self.run.check_cancelled()
        key = object()
        self._events.start("model", key, request_context.model.model_name)
        try:
            response = await handler(request_context)
        except SkipModelRequest:
            self._events.discard("model", key)
            raise
        except BaseException as error:
            self._events.model_end(key, error=error)
            raise
        self._events.model_end(key, model=response.model_name, usage=response.usage)
        return response

    async def wrap_tool_execute(
        self,
        ctx: RunContext[Any],
        *,
        call: ToolCallPart,
        tool_def: ToolDefinition,
        args: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> Any:
        self.run.check_cancelled()
        key = object()
        self._events.start("tool", key, call.tool_name, args)
        try:
            result = await handler(args)
        except (ApprovalRequired, CallDeferred, SkipToolExecution):
            # Framework control flow is not a dependency failure.
            self._events.discard("tool", key)
            raise
        except BaseException as error:
            self._events.tool_end(key, error=error)
            raise
        self._events.tool_end(key)
        return result
