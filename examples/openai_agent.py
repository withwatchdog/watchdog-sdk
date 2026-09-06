"""Requires openai-agents and OPENAI_API_KEY; calls a real model when executed."""

import asyncio
import os

from agents import Agent, Runner, RunConfig
from watchdog_agent import Watchdog
from watchdog_agent.openai_agents import WatchdogHooks


async def main() -> None:
    agent = Agent(name="Report writer", instructions="Write a concise competitor report.", model=os.environ["OPENAI_MODEL"])
    async with Watchdog() as watchdog:
        async with watchdog.run(os.getenv("WATCHDOG_JOB", "sdk-example")) as run:
            result = await Runner.run(
                agent,
                "Draft a fictional competitor report for a test integration.",
                hooks=WatchdogHooks(run),
                run_config=RunConfig(trace_include_sensitive_data=False),
            )
            # Persist the result in your own application, then emit the real outcome.
            run.progress("Draft ready")
            print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
