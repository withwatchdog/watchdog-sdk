import asyncio
from types import SimpleNamespace
import unittest

from _support import CaptureCase

try:
    from pydantic_ai import Agent
    from pydantic_ai.exceptions import ApprovalRequired, CallDeferred, ModelRetry, SkipToolExecution
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.usage import RequestUsage
    from watchdog_agent.pydantic_ai import WatchdogCapability

    AVAILABLE = True
except ImportError:
    AVAILABLE = False


@unittest.skipUnless(AVAILABLE, "Install the pydanticai optional dependency")
class PydanticAITests(CaptureCase):
    async def test_real_agent_parallel_tools_and_per_response_usage(self):
        def model_fn(messages, info):
            if any(isinstance(part, ToolReturnPart) for part in messages[-1].parts):
                return ModelResponse(
                    [TextPart("private-final")],
                    model_name="test-model",
                    usage=RequestUsage(input_tokens=20, output_tokens=4),
                )
            return ModelResponse(
                [
                    ToolCallPart("lookup", {"query": "private-one"}, tool_call_id="one"),
                    ToolCallPart("lookup", {"query": "private-two"}, tool_call_id="two"),
                ],
                model_name="test-model",
                usage=RequestUsage(input_tokens=10, output_tokens=2),
            )

        agent = Agent(FunctionModel(model_fn))

        @agent.tool_plain
        async def lookup(query: str) -> str:
            await asyncio.sleep(0)
            return "private-output"

        with self.client.run("job") as run:
            result = await agent.run("private-prompt", capabilities=[WatchdogCapability(run)])
            self.assertEqual(result.output, "private-final")
            run.outcome("saved")
        self.assertEqual(sum(row["input_tokens"] for row in self.records("llm.completed")), 30)
        tools = self.records("tool.completed")
        self.assertEqual(len(tools), 2)
        self.assertNotEqual(tools[0]["fingerprint"], tools[1]["fingerprint"])
        self.assertEqual(self.events[-1]["type"], "run.completed")
        self.assert_private()

    async def test_real_streaming_records_final_usage_once(self):
        agent = Agent(TestModel(custom_output_text="private-output"))
        with self.client.run("job") as run:
            async with agent.run_stream("private-prompt", capabilities=[WatchdogCapability(run)]) as result:
                output = await result.get_output()
                self.assertEqual(output, "private-output")
        self.assertEqual(len(self.records("llm.completed")), 1)
        self.assertTrue(self.records("llm.completed")[0]["metadata"]["usage_available"])
        self.assert_private()

    def test_real_sync_agent(self):
        agent = Agent(TestModel())
        with self.client.run("job") as run:
            agent.run_sync("private-prompt", capabilities=[WatchdogCapability(run)])
        self.assertEqual(len(self.records("llm.completed")), 1)
        self.assertEqual(self.events[-1]["type"], "run.completed")

    async def test_real_tool_error_is_preserved(self):
        original = ValueError("private-error")
        agent = Agent(TestModel())

        @agent.tool_plain
        def broken() -> str:
            raise original

        with self.assertRaises(ValueError) as caught:
            with self.client.run("job") as run:
                await agent.run("private-prompt", capabilities=[WatchdogCapability(run)])
        self.assertIs(caught.exception, original)
        self.assertEqual(self.records("tool.completed")[0]["status"], "error")
        self.assertEqual(self.events[-1]["type"], "run.failed")
        self.assert_private()

    async def test_real_model_failure_is_preserved(self):
        original = ValueError("private-error")

        def model_fn(messages, info):
            raise original

        agent = Agent(FunctionModel(model_fn))
        with self.assertRaises(ValueError) as caught:
            with self.client.run("job") as run:
                await agent.run("private-prompt", capabilities=[WatchdogCapability(run)])
        self.assertIs(caught.exception, original)
        self.assertEqual(self.records("llm.completed")[0]["metadata"]["status"], "error")
        self.assert_private()

    async def test_control_flow_is_not_a_tool_failure_and_original_is_raised(self):
        for original in (ApprovalRequired(), CallDeferred(), SkipToolExecution("private-result")):

            async def handler(args):
                raise original

            with self.client.run("job") as run:
                capability = WatchdogCapability(run)
                with self.assertRaises(type(original)) as caught:
                    await capability.wrap_tool_execute(
                        None, call=ToolCallPart("lookup", {}), tool_def=None, args={}, handler=handler
                    )
                self.assertIs(caught.exception, original)
                self.assertEqual(capability._events._count, 0)
        self.assertEqual(self.records("tool.completed"), [])

    async def test_model_retry_is_a_failed_attempt_not_automatic_progress(self):
        original = ModelRetry("private-retry")

        async def handler(args):
            raise original

        with self.client.run("job") as run:
            with self.assertRaises(ModelRetry):
                await WatchdogCapability(run).wrap_tool_execute(
                    None, call=ToolCallPart("lookup", {}), tool_def=None, args={"q": "private-query"}, handler=handler
                )
        self.assertEqual(self.records("tool.completed")[0]["status"], "error")
        self.assert_private()

    async def test_cancellation_propagates_through_wrapper_and_closes_run(self):
        async def handler(context):
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            async with self.client.run("job") as run:
                await WatchdogCapability(run).wrap_model_request(
                    None, request_context=SimpleNamespace(model=SimpleNamespace(model_name="test")), handler=handler
                )
        self.assertEqual(self.events[-1]["type"], "run.cancelled")

    async def test_observer_failure_does_not_replace_application_result(self):
        agent = Agent(TestModel(custom_output_text="private-output"))
        with self.client.run("job") as run:

            def broken(**kwargs):
                raise RuntimeError("private-instrumentation-error")

            run.usage = broken
            result = await agent.run("private-prompt", capabilities=[WatchdogCapability(run)])
            self.assertEqual(result.output, "private-output")
        self.assertEqual(self.client.stats["dropped"], 1)
        self.assertEqual(self.events[-1]["type"], "run.completed")
