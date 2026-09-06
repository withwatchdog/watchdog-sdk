"""Exercises the real OpenAI Agents Runner with a deterministic local model.

The optional dependency must be installed to run these tests. No provider network
request or credential is used; tracing is disabled explicitly.
"""

import asyncio
import inspect
import json
import unittest

from watchdog_agent import Watchdog

try:
    from agents import Agent, Runner, RunConfig, function_tool
    from agents.exceptions import MaxTurnsExceeded
    from agents.items import ModelResponse
    from agents.lifecycle import RunHooksBase
    from agents.models.interface import Model
    from agents.usage import Usage
    from openai.types.responses import ResponseFunctionToolCall, ResponseOutputMessage, ResponseOutputText
    from watchdog_agent.openai_agents import WatchdogHooks
    AVAILABLE = True
except ImportError:
    AVAILABLE = False
    Model = object


class ScriptedModel(Model):
    model = "scripted-local-model"

    def __init__(self, responses, delay=0):
        self.responses = list(responses)
        self.delay = delay

    async def get_response(self, *args, **kwargs):
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.responses.pop(0)

    async def stream_response(self, *args, **kwargs):
        raise NotImplementedError("Streaming is outside this deterministic integration test")
        yield  # Declare the async generator required by the model interface.


def message_response(text="private-final-output"):
    return ModelResponse(
        output=[ResponseOutputMessage(id="msg-local", type="message", role="assistant", status="completed", content=[ResponseOutputText(type="output_text", text=text, annotations=[])])],
        usage=Usage(requests=1, input_tokens=200, output_tokens=20, total_tokens=220),
        response_id="response-final",
    )


@unittest.skipUnless(AVAILABLE, "Install the openai optional dependency to run real Runner tests")
class RealRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_adapter_covers_installed_lifecycle_signatures(self):
        for name in ("on_agent_start", "on_agent_end", "on_handoff", "on_llm_start", "on_llm_end", "on_tool_start", "on_tool_end"):
            original = list(inspect.signature(getattr(RunHooksBase, name)).parameters)
            implemented = list(inspect.signature(getattr(WatchdogHooks, name)).parameters)
            self.assertEqual(implemented[:-1], original)
            self.assertEqual(implemented[-1], "kwargs")

    async def test_real_runner_parallel_tools_usage_and_privacy(self):
        @function_tool
        async def lookup(query: str) -> str:
            """Return an example record.

            Args:
                query: The example record selector.
            """
            await asyncio.sleep(0)
            return "private-tool-result-" + query

        tool_response = ModelResponse(
            output=[
                ResponseFunctionToolCall(id="fc-one", type="function_call", call_id="call-one", name="lookup", arguments='{"query":"private-a"}'),
                ResponseFunctionToolCall(id="fc-two", type="function_call", call_id="call-two", name="lookup", arguments='{"query":"private-b"}'),
            ],
            usage=Usage(requests=1, input_tokens=100, output_tokens=10, total_tokens=110),
            response_id="response-tools",
        )
        model = ScriptedModel([tool_response, message_response()])
        agent = Agent(name="Example", instructions="private-system-prompt", model=model, tools=[lookup])
        events = []
        async with Watchdog("test", _transport=lambda method, path, body: events.append(json.loads(body))) as client:
            async with client.run("job") as run:
                result = await Runner.run(agent, "private-input", hooks=WatchdogHooks(run), run_config=RunConfig(tracing_disabled=True))
                self.assertEqual(result.final_output, "private-final-output")
                run.outcome("record_created")
        self.assertEqual(events[0]["type"], "run.started")
        self.assertEqual(events[-1]["type"], "run.completed")
        tools = [e for e in events if e["type"] == "tool.completed"]
        usage = [e for e in events if e["type"] == "llm.completed"]
        self.assertEqual(len(tools), 2)
        self.assertNotEqual(tools[0]["data"]["fingerprint"], tools[1]["data"]["fingerprint"])
        self.assertEqual(sum(e["data"]["input_tokens"] for e in usage), 300)
        self.assertEqual(sum(e["data"]["output_tokens"] for e in usage), 30)
        self.assertTrue(all(e["data"]["model"] == "scripted-local-model" for e in usage))
        self.assertNotIn("private-", json.dumps(events))
        self.assertNotIn("progress", [e["type"] for e in events])

    async def test_runner_error_is_preserved_and_finishes_failed(self):
        model = ScriptedModel([message_response()])
        events = []
        async with Watchdog("test", _transport=lambda method, path, body: events.append(json.loads(body))) as client:
            with self.assertRaises(MaxTurnsExceeded):
                async with client.run("job") as run:
                    await Runner.run(Agent(name="Example", model=model), "input", max_turns=0, hooks=WatchdogHooks(run), run_config=RunConfig(tracing_disabled=True))
        self.assertEqual(events[-1]["type"], "run.failed")
        self.assertEqual(events[-1]["data"]["metadata"]["error_type"], "MaxTurnsExceeded")

    async def test_async_cancellation_interrupts_real_runner(self):
        events = []
        commands = []
        def transport(method, path, body):
            if method == "GET":
                return {"commands": commands}
            events.append((path, json.loads(body)))
            return {}
        model = ScriptedModel([message_response()], delay=3)
        async with Watchdog("test", _transport=transport, poll_interval=0.01) as client:
            async def runtime():
                async with client.run("job", cancellable=True) as run:
                    commands.append({"id": "stop-real-runner", "type": "cancel"})
                    await Runner.run(Agent(name="Example", model=model), "input", hooks=WatchdogHooks(run), run_config=RunConfig(tracing_disabled=True))
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(runtime(), timeout=1)
        run_events = [body for path, body in events if path == "/api/v1/events"]
        self.assertEqual(run_events[-1]["type"], "run.cancelled")
        self.assertEqual(events[-1][1]["status"], "acknowledged")


if __name__ == "__main__":
    unittest.main()
