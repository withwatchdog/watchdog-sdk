"""A real LangGraph with a local fake model; no provider credentials needed."""

import asyncio
import os
from typing import TypedDict

from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from watchdog_agent import Watchdog
from watchdog_agent.langgraph import WatchdogCallbackHandler


class State(TypedDict):
    answer: str


@tool
def lookup() -> str:
    """Return a synthetic record."""
    return "Example record"


async def main() -> None:
    model = FakeMessagesListChatModel(responses=[AIMessage(content="Synthetic report ready.")])

    async def work(state: State, config: RunnableConfig) -> State:
        # Explicit config propagation also works on Python 3.10.
        await lookup.ainvoke({}, config=config)
        response = await model.ainvoke("Prepare the example.", config=config)
        return {"answer": str(response.content)}

    builder = StateGraph(State)
    builder.add_node("work", work)
    builder.add_edge(START, "work")
    builder.add_edge("work", END)
    graph = builder.compile()

    async with Watchdog() as watchdog:
        async with watchdog.run(os.getenv("WATCHDOG_JOB", "sdk-example")) as run:
            result = await graph.ainvoke({"answer": ""}, config={"callbacks": [WatchdogCallbackHandler(run)]})
            print(result["answer"])
            run.progress("Synthetic report ready")


if __name__ == "__main__":
    asyncio.run(main())
