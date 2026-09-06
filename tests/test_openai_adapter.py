"""Contract tests with minimal framework-shaped objects; no model calls occur."""

import json
from types import SimpleNamespace
import unittest

try:
    from watchdog_agent.openai_agents import WatchdogHooks
    AVAILABLE = True
except ImportError:
    AVAILABLE = False

from watchdog_agent import Watchdog


@unittest.skipUnless(AVAILABLE, "Install the openai optional dependency")
class HookTests(unittest.IsolatedAsyncioTestCase):
    async def test_hooks_ignore_sensitive_inputs_and_do_not_mark_business_progress(self):
        events = []
        def transport(method, path, body):
            events.append(json.loads(body))
            return {}
        with Watchdog("test", _transport=transport) as client:
            async with client.run("job") as run:
                hooks = WatchdogHooks(run)
                context = SimpleNamespace(tool_call_id="call-1", tool_arguments='{"query":"private-query"}')
                agent = SimpleNamespace(model="configured-model")
                tool = SimpleNamespace(name="search")
                await hooks.on_agent_start(context, agent)
                await hooks.on_llm_start(context, agent, "private-system-prompt", ["private-input"])
                await hooks.on_llm_end(context, agent, SimpleNamespace(usage=SimpleNamespace(input_tokens=100, output_tokens=15), output="private-completion"))
                await hooks.on_tool_start(context, agent, tool)
                await hooks.on_tool_end(context, agent, tool, "private-tool-result")
                await hooks.on_handoff(context, agent, agent)
                await hooks.on_agent_end(context, agent, "private-output")
        self.assertEqual([e["type"] for e in events], ["run.started", "llm.completed", "tool.completed", "run.completed"])
        self.assertNotIn("private-", json.dumps(events))
        self.assertEqual(events[1]["data"]["input_tokens"], 100)
        self.assertEqual(events[1]["data"]["output_tokens"], 15)
        self.assertEqual(len(events[2]["data"]["fingerprint"]), 64)

    async def test_parallel_tool_calls_keep_distinct_fingerprints(self):
        events = []
        with Watchdog("test", _transport=lambda method, path, body: events.append(json.loads(body))) as client:
            async with client.run("job") as run:
                hooks = WatchdogHooks(run)
                tool = SimpleNamespace(name="search")
                first = SimpleNamespace(tool_call_id="one", tool_arguments='{"query":"a"}')
                second = SimpleNamespace(tool_call_id="two", tool_arguments='{"query":"b"}')
                await hooks.on_tool_start(first, None, tool)
                await hooks.on_tool_start(second, None, tool)
                await hooks.on_tool_end(second, None, tool, "unused")
                await hooks.on_tool_end(first, None, tool, "unused")
                expected = [run.fingerprint("search", second.tool_arguments), run.fingerprint("search", first.tool_arguments)]
        actual = [e["data"]["fingerprint"] for e in events if e["type"] == "tool.completed"]
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
