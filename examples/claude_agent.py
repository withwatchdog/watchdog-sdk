"""Requires Claude authentication. Executes a real, small read-only query."""

import asyncio
from contextlib import aclosing
import os

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from watchdog_agent import Watchdog
from watchdog_agent.claude_agents import WatchdogClaudeMonitor


async def main() -> None:
    async with Watchdog() as watchdog:
        async with watchdog.run(os.getenv("WATCHDOG_JOB", "sdk-example")) as run:
            monitor = WatchdogClaudeMonitor(run)
            options = ClaudeAgentOptions(
                hooks=monitor.hooks(),
                tools=[],  # This example grants no filesystem or shell tools.
                max_turns=1,
            )
            async with aclosing(
                monitor.stream(
                    query(
                        prompt="Reply with a one-sentence fictional weather report.",
                        options=options,
                    )
                )
            ) as stream:
                async for message in stream:
                    if isinstance(message, ResultMessage):
                        print("Claude query failed" if message.is_error else message.result)
            # In a real job, emit progress/outcomes only after your work succeeds.


if __name__ == "__main__":
    asyncio.run(main())
