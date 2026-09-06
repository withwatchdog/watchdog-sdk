"""A real PydanticAI agent with TestModel; no provider credentials needed."""

import asyncio
import os

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from watchdog_agent import Watchdog
from watchdog_agent.pydantic_ai import WatchdogCapability


async def main() -> None:
    agent = Agent(TestModel())

    @agent.tool_plain
    def lookup() -> str:
        """Return a synthetic record."""
        return "Example record"

    async with Watchdog() as watchdog:
        async with watchdog.run(os.getenv("WATCHDOG_JOB", "sdk-example")) as run:
            result = await agent.run("Prepare an example report.", capabilities=[WatchdogCapability(run)])
            print(result.output)
            run.progress("Synthetic report ready")


if __name__ == "__main__":
    asyncio.run(main())
