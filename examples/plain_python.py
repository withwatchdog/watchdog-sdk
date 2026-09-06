"""Plain Python only. No model calls or third-party dependencies."""

import os

from watchdog_agent import Watchdog


def main() -> None:
    with Watchdog() as watchdog:
        with watchdog.run(os.getenv("WATCHDOG_JOB", "sdk-example")) as run:
            rows = []
            for index in range(3):
                with run.tool_call("synthetic.read", arguments={"index": index}):
                    rows.append(index * 2)
                # A completed work unit, not just another tool/model invocation.
                run.progress("Row processed", completed=index + 1, total=3)
            print(rows)
            run.outcome("rows_processed")


if __name__ == "__main__":
    main()
