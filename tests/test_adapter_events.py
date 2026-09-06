import concurrent.futures
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from _support import CaptureCase
from watchdog_agent._adapter import AdapterEvents


class AdapterEventsTests(CaptureCase):
    def test_fifo_timing_and_error_fingerprints_without_raw_arguments(self):
        with self.client.run("job") as run:
            events = AdapterEvents(run, "test")
            with patch("watchdog_agent._adapter.time.monotonic", side_effect=[10, 11, 13, 16]):
                events.start("tool", "shared", "lookup", {"q": "private-one"})
                events.start("tool", "shared", "lookup", {"q": "private-two"})
                events.tool_end("shared")
                events.tool_end("shared", error=ValueError("private-error"))
            expected = run.fingerprint("lookup", {"q": "private-two"}, "error")
        tools = self.records("tool.completed")
        self.assertEqual([row["duration_ms"] for row in tools], [3000, 5000])
        self.assertEqual(tools[1]["fingerprint"], expected)
        self.assertEqual(tools[1]["metadata"]["error_type"], "ValueError")
        self.assert_private()

    def test_total_pending_starts_are_bounded_even_for_same_key(self):
        with self.client.run("job") as run:
            events = AdapterEvents(run, "test")
            for _ in range(1100):
                events.start("tool", "same", "lookup")
            self.assertEqual(events._count, 1024)
            self.assertEqual(self.client.stats["dropped"], 76)
            for _ in range(1024):
                events.discard("tool", "same")
            self.assertEqual(events._count, 0)
            self.assertEqual(len(events._pending), 0)

    def test_concurrent_completions_keep_correlation(self):
        with self.client.run("job") as run:
            events = AdapterEvents(run, "test")

            def call(index):
                events.start("tool", index, "lookup", {"index": index})
                events.tool_end(index)

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(call, range(80)))
            self.assertEqual(events._count, 0)
        tools = self.records("tool.completed")
        self.assertEqual(len(tools), 80)
        self.assertEqual(len({row["fingerprint"] for row in tools}), 80)

    def test_observation_failure_does_not_escape(self):
        with self.client.run("job") as run:
            events = AdapterEvents(run, "test")
            with patch.object(run, "tool", side_effect=RuntimeError("private-secret")):
                events.tool_end("missing")
            with patch.object(run, "usage", side_effect=RuntimeError("private-secret")):
                events.model_end("missing")
            self.assertEqual(self.client.stats["dropped"], 2)
        self.assertEqual(self.events[-1]["type"], "run.completed")

    def test_unknown_usage_is_explicit_and_does_not_invent_cost(self):
        with self.client.run("job") as run:
            AdapterEvents(run, "test").model_end("missing")
        usage = self.records("llm.completed")[0]
        self.assertFalse(usage["metadata"]["usage_available"])
        self.assertNotIn("cost_usd", usage)
        self.assertNotIn("duration_ms", usage["metadata"])

    def test_base_import_does_not_import_frameworks_and_missing_extras_are_helpful(self):
        script = """
import importlib
import importlib.abc
import sys
class BlockFrameworks(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'agents', 'openai', 'claude_agent_sdk', 'langchain_core', 'langgraph', 'pydantic_ai'}:
            raise ModuleNotFoundError(fullname)
sys.meta_path.insert(0, BlockFrameworks())
import watchdog_agent
assert not any(name in sys.modules for name in ('agents', 'claude_agent_sdk', 'langchain_core', 'pydantic_ai'))
for module, extra in [('openai_agents', 'openai'), ('claude_agents', 'claude'), ('langgraph', 'langgraph'), ('pydantic_ai', 'pydanticai')]:
    try:
        importlib.import_module('watchdog_agent.' + module)
    except ImportError as error:
        assert 'watchdog-agent-sdk[' + extra + ']' in str(error)
    else:
        raise AssertionError(module)
print('dependency-free import and four optional import errors: OK')
"""
        result = subprocess.run(
            [sys.executable, "-c", script], cwd=Path(__file__).parents[1], text=True, capture_output=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("OK", result.stdout)

    def test_plain_lifecycle_has_no_implicit_progress(self):
        with self.client.run("job"):
            pass
        self.assertEqual([event["type"] for event in self.events], ["run.started", "run.completed"])

    def test_two_runs_share_no_fingerprints_or_span_state(self):
        with self.client.run("first") as first, self.client.run("second") as second:
            a, b = AdapterEvents(first, "test"), AdapterEvents(second, "test")
            a.start("tool", "same", "lookup", {"id": 1})
            b.start("tool", "same", "lookup", {"id": 1})
            b.tool_end("same")
            a.tool_end("same")
        tools = [event for event in self.events if event["type"] == "tool.completed"]
        self.assertEqual([event["run_id"] for event in tools], [second.id, first.id])
        self.assertNotEqual(tools[0]["data"]["fingerprint"], tools[1]["data"]["fingerprint"])
