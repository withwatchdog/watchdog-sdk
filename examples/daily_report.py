"""Run without an LLM key; demonstrates a real ingestion integration.

Set WATCHDOG_URL and WATCHDOG_API_KEY, then create the daily-report job in Watchdog.
"""

from watchdog_agent import Watchdog


def main() -> None:
    with Watchdog() as watchdog:
        with watchdog.run("daily-report", cancellable=True) as run:
            with run.tool_call("competitors.search", arguments={"sector": "software"}):
                competitors = ["Example A", "Example B"]
            run.progress("Research complete", completed=len(competitors), total=len(competitors))
            # Report real measured usage from your model provider; no model call occurs here.
            run.outcome("report_created", metadata={"record_id": "example-report"})
            run.check_cancelled()
            # Only report business outcomes after the corresponding operation succeeds.
            run.outcome("report_posted")
    print("Delivery counters:", watchdog.stats)


if __name__ == "__main__":
    main()
