import asyncio
from typing import TypedDict
import unittest
from uuid import uuid4

from _support import CaptureCase

try:
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage, ToolMessage
    from langchain_core.outputs import ChatGeneration, Generation, LLMResult
    from langchain_core.tools import tool
    from langgraph.graph import END, START, StateGraph
    from watchdog_agent.langgraph import WatchdogCallbackHandler

    AVAILABLE = True
except ImportError:
    AVAILABLE = False


class State(TypedDict):
    answer: str


@unittest.skipUnless(AVAILABLE, "Install the langgraph optional dependency")
class LangGraphTests(CaptureCase):
    def model(self):
        return FakeMessagesListChatModel(
            responses=[
                AIMessage(
                    content="private-model-result",
                    usage_metadata={"input_tokens": 13, "output_tokens": 7, "total_tokens": 20},
                )
            ]
        )

    async def test_real_sync_and_async_graph_propagate_callbacks_and_keep_parent_lifecycle(self):
        @tool
        def lookup(query: str) -> str:
            """Return a synthetic record."""
            return "private-tool-output"

        for asynchronous in (False, True):
            model = self.model()

            async def async_node(state, config):
                answer, _ = await asyncio.gather(
                    model.ainvoke("private-prompt", config=config),
                    lookup.ainvoke({"query": "private-query"}, config=config),
                )
                return {"answer": answer.content}

            def sync_node(state, config):
                answer = model.invoke("private-prompt", config=config)
                lookup.invoke({"query": "private-query"}, config=config)
                return {"answer": answer.content}

            graph = StateGraph(State)
            graph.add_node("work", async_node if asynchronous else sync_node)
            graph.add_edge(START, "work")
            graph.add_edge("work", END)
            compiled = graph.compile()
            with self.client.run("job") as run:
                config = {"callbacks": [WatchdogCallbackHandler(run)]}
                output = (
                    await compiled.ainvoke({"answer": ""}, config)
                    if asynchronous
                    else compiled.invoke({"answer": ""}, config)
                )
                self.assertEqual(output["answer"], "private-model-result")
                # Nested graph completion must leave the enclosing run open.
                run.outcome("saved")
        self.assertEqual(len(self.records("run.completed")), 2)
        self.assertEqual(len(self.records("outcome")), 2)
        self.assertEqual(len(self.records("tool.completed")), 2)
        self.assertEqual(sum(row["input_tokens"] for row in self.records("llm.completed")), 26)
        self.assert_private()

    async def test_real_tool_error_and_graph_exception_preserve_original(self):
        original = ValueError("private-tool-error")

        @tool
        def broken(query: str) -> str:
            """Fail deliberately."""
            raise original

        with self.assertRaises(ValueError) as caught:
            with self.client.run("job") as run:
                await broken.ainvoke({"query": "private-query"}, config={"callbacks": [WatchdogCallbackHandler(run)]})
        self.assertIs(caught.exception, original)
        self.assertEqual(self.records("tool.completed")[0]["status"], "error")
        self.assertEqual(self.events[-1]["type"], "run.failed")
        self.assert_private()

    def test_tool_message_error_status_not_sensitive_output_is_used(self):
        with self.client.run("job") as run:
            callbacks = WatchdogCallbackHandler(run)
            call_id = uuid4()
            callbacks.on_tool_start({"name": "lookup"}, "private-args", run_id=call_id)
            callbacks.on_tool_end(ToolMessage("private-error", tool_call_id="one", status="error"), run_id=call_id)
        self.assertEqual(self.records("tool.completed")[0]["status"], "error")
        self.assert_private()

    def test_llm_failure_preserves_only_exception_type(self):
        with self.client.run("job") as run:
            callbacks = WatchdogCallbackHandler(run)
            call_id = uuid4()
            callbacks.on_llm_start({}, ["private-prompt"], run_id=call_id, metadata={"ls_model_name": "test-model"})
            callbacks.on_llm_error(ValueError("private-error"), run_id=call_id)
        row = self.records("llm.completed")[0]
        self.assertEqual(row["model"], "test-model")
        self.assertEqual(row["metadata"]["status"], "error")
        self.assertFalse(row["metadata"]["usage_available"])
        self.assert_private()

    def test_canonical_usage_wins_over_provider_totals_and_n_best_is_not_double_counted(self):
        message = AIMessage(
            content="private-result", usage_metadata={"input_tokens": 4, "output_tokens": 2, "total_tokens": 6}
        )
        response = LLMResult(
            generations=[[ChatGeneration(message=message), ChatGeneration(message=message)]],
            llm_output={"token_usage": {"prompt_tokens": 99, "completion_tokens": 99}},
        )
        with self.client.run("job") as run:
            WatchdogCallbackHandler(run).on_llm_end(response, run_id=uuid4())
        self.assertEqual(self.records("llm.completed")[0]["input_tokens"], 4)

    def test_non_chat_usage_fallback(self):
        response = LLMResult(
            generations=[[Generation(text="private-result")]],
            llm_output={"token_usage": {"prompt_tokens": 8, "completion_tokens": 3}, "model_name": "test"},
        )
        with self.client.run("job") as run:
            WatchdogCallbackHandler(run).on_llm_end(response, run_id=uuid4())
        self.assertEqual(self.records("llm.completed")[0]["input_tokens"], 8)
        self.assert_private()

    def test_malformed_provider_usage_is_fail_open(self):
        response = LLMResult(generations=[], llm_output={"token_usage": {"prompt_tokens": "invalid"}})
        with self.client.run("job") as run:
            WatchdogCallbackHandler(run).on_llm_end(response, run_id=uuid4())
        self.assertEqual(self.client.stats["dropped"], 1)
        self.assertEqual(self.events[-1]["type"], "run.completed")
