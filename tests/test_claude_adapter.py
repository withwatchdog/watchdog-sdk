from contextlib import aclosing

from _support import CaptureCase

try:
    from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, HookMatcher, ResultMessage
    from watchdog_agent.claude_agents import WatchdogClaudeMonitor

    AVAILABLE = True
except ImportError:
    AVAILABLE = False

import unittest


async def messages(*items):
    for item in items:
        yield item


def result(**changes):
    fields = dict(
        subtype="success",
        duration_ms=10,
        duration_api_ms=5,
        is_error=False,
        num_turns=1,
        session_id="private-session",
        usage={"input_tokens": 11, "output_tokens": 3},
        total_cost_usd=100,
        result="private-output",
    )
    fields.update(changes)
    return ResultMessage(**fields)


@unittest.skipUnless(AVAILABLE, "Install the claude optional dependency")
class ClaudeTests(CaptureCase):
    async def test_real_options_and_hook_matchers_preserve_permission_hooks(self):
        async def permission(data, tool_use_id, context):
            return {"decision": "block"}

        existing = {"PreToolUse": [HookMatcher(matcher="Bash", hooks=[permission])]}
        with self.client.run("job") as run:
            monitor = WatchdogClaudeMonitor(run)
            options = ClaudeAgentOptions(hooks=monitor.hooks(existing))
            self.assertEqual(len(existing["PreToolUse"]), 1)
            self.assertIs(options.hooks["PreToolUse"][0].hooks[0], permission)
            self.assertEqual(len(options.hooks["PreToolUse"]), 2)

    async def test_tool_success_failure_concurrency_and_privacy(self):
        with self.client.run("job") as run:
            monitor = WatchdogClaudeMonitor(run)
            hooks = monitor.hooks()

            async def emit(event, call, value):
                data = dict(
                    hook_event_name=event,
                    session_id="private-session",
                    cwd="private-path",
                    transcript_path="private-transcript",
                    tool_name="lookup",
                    tool_input={"q": value},
                    tool_response="private-result",
                    error="private-error",
                )
                observed = await hooks[event][0].hooks[0](data, call, {})
                self.assertEqual(observed, {})

            await emit("PreToolUse", "one", "private-one")
            await emit("PreToolUse", "two", "private-two")
            await emit("PostToolUseFailure", "two", "private-two")
            await emit("PostToolUse", "one", "private-one")
            expected = run.fingerprint("lookup", {"q": "private-two"}, "error")
        tools = self.records("tool.completed")
        self.assertEqual([row["status"] for row in tools], ["error", "success"])
        self.assertEqual(tools[0]["fingerprint"], expected)
        self.assert_private()

    async def test_stream_passes_original_messages_and_counts_per_turn_not_session_usage(self):
        turns = [result(), result(total_cost_usd=200, model_usage={"model": {"inputTokens": 9000}})]
        assistant = AssistantMessage(content=[], model="example-model")
        with self.client.run("job") as run:
            monitor = WatchdogClaudeMonitor(run)
            observed = [item async for item in monitor.stream(messages(assistant, *turns))]
            self.assertIs(observed[0], assistant)
            self.assertIs(observed[1], turns[0])
        usage = self.records("llm.completed")
        self.assertEqual(sum(item["input_tokens"] for item in usage), 22)
        self.assertTrue(all(item["metadata"]["usage_scope"] == "main_agent_turn" for item in usage))
        self.assertTrue(all("cost_usd" not in item for item in usage))
        self.assertEqual(self.events[-1]["type"], "run.completed")
        self.assert_private()

    async def test_error_result_cannot_be_overwritten_by_normal_context_exit(self):
        with self.client.run("job") as run:
            monitor = WatchdogClaudeMonitor(run)
            observed = [item async for item in monitor.stream(messages(result(is_error=True)))]
            self.assertTrue(observed[0].is_error)
        self.assertEqual(self.events[-1]["type"], "run.failed")
        self.assertEqual(len(self.records("run.failed")), 1)
        self.assertEqual(self.records("run.completed"), [])

    async def test_error_subtype_and_interrupted_result(self):
        for message, expected in [
            (result(subtype="error_max_turns"), "run.failed"),
            (result(terminal_reason="aborted_tools"), "run.cancelled"),
        ]:
            with self.client.run("job") as run:
                _ = [item async for item in WatchdogClaudeMonitor(run).stream(messages(message))]
            self.assertEqual(self.events[-1]["type"], expected)

    async def test_missing_terminal_result_is_failure(self):
        with self.client.run("job") as run:
            _ = [item async for item in WatchdogClaudeMonitor(run).stream(messages())]
        self.assertEqual(self.events[-1]["data"]["metadata"]["error_type"], "ClaudeIncompleteStream")

    async def test_explicit_early_close_is_failure_and_closes_source(self):
        closed = []

        async def source():
            try:
                yield AssistantMessage([], "model")
                yield result()
            finally:
                closed.append(True)

        with self.client.run("job") as run:
            async with aclosing(WatchdogClaudeMonitor(run).stream(source())) as stream:
                async for _ in stream:
                    break
            self.assertEqual(closed, [True])
        self.assertEqual(self.events[-1]["type"], "run.failed")

    async def test_close_after_terminal_result_is_not_a_false_failure(self):
        with self.client.run("job") as run:
            async with aclosing(WatchdogClaudeMonitor(run).stream(messages(result()))) as stream:
                async for _ in stream:
                    break
        self.assertEqual(self.events[-1]["type"], "run.completed")

    async def test_original_stream_exception_propagates(self):
        original = ValueError("private-api-error")

        async def failed():
            raise original
            yield

        with self.assertRaises(ValueError) as caught:
            with self.client.run("job") as run:
                _ = [item async for item in WatchdogClaudeMonitor(run).stream(failed())]
        self.assertIs(caught.exception, original)
        self.assertEqual(self.events[-1]["data"]["metadata"]["error_type"], "ValueError")
        self.assert_private()

    async def test_cached_input_tokens_are_counted_once(self):
        with self.client.run("job") as run:
            usage = {
                "input_tokens": 2,
                "cache_read_input_tokens": 10,
                "cache_creation_input_tokens": 5,
                "output_tokens": 3,
            }
            _ = [item async for item in WatchdogClaudeMonitor(run).stream(messages(result(usage=usage)))]
        self.assertEqual(self.records("llm.completed")[0]["input_tokens"], 17)

    async def test_bad_usage_does_not_hide_reported_failure_or_change_messages(self):
        message = result(is_error=True, usage={"input_tokens": "invalid"})
        with self.client.run("job") as run:
            observed = [item async for item in WatchdogClaudeMonitor(run).stream(messages(message))]
        self.assertIs(observed[0], message)
        self.assertEqual(self.events[-1]["type"], "run.failed")
        self.assertEqual(self.client.stats["dropped"], 1)
