# Minimal integration examples

Register a job (for example `sdk-example`) in Watchdog. Keep progress monitoring
disabled for lifecycle-only coverage; set a maximum runtime appropriate to your
application. Add a verified alert destination in the dashboard.

Download and extract the [0.2.0 source archive from PyPI](https://pypi.org/project/watchdog-agent-sdk/0.2.0/#files).
From its extracted directory, install the SDK and set:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install watchdog-agent-sdk==0.2.0
export WATCHDOG_URL="https://withwatchdog.com"
export WATCHDOG_API_KEY="your-telemetry-key"
export WATCHDOG_JOB="sdk-example"
python examples/plain_python.py
```

The examples can also be copied into your own application. Version 0.1.0 does
not include the new adapters. For SDK development from a checkout, install
`.[EXTRA]` instead of the corresponding published package below.

| Example | Install from PyPI | Provider access |
| --- | --- | --- |
| `plain_python.py` | `python -m pip install watchdog-agent-sdk==0.2.0` | None |
| `openai_agent.py` | `python -m pip install 'watchdog-agent-sdk[openai]==0.2.0'` | `OPENAI_API_KEY` and `OPENAI_MODEL`; makes a real model request |
| `claude_agent.py` | `python -m pip install 'watchdog-agent-sdk[claude]==0.2.0'` | Claude's supported authentication, e.g. `ANTHROPIC_API_KEY`; makes a real query |
| `langgraph_agent.py` | `python -m pip install 'watchdog-agent-sdk[langgraph]==0.2.0'` | None; official local fake model |
| `pydantic_ai_agent.py` | `python -m pip install 'watchdog-agent-sdk[pydanticai]==0.2.0'` | None; official TestModel |

Run each with `python examples/<filename>`. Expected result: one started run,
tool/model completion metadata where applicable, and one completed run on normal
exit. Exceptions that leave the run context produce a failed run and propagate
unchanged. The plain, LangGraph and PydanticAI examples report explicit milestones.
Claude tool hooks are configured but its example grants no tools.

The fake models are demonstrations of integration, not tests of model accuracy.
For an offline smoke test, unset `WATCHDOG_API_KEY`; Watchdog delivery is then
disabled. OpenAI and Claude examples still require provider access when executed.
The test suite exercises their hooks with local models/messages instead.

Do not wrap a tool with `run.tool_call()` when an adapter already records that
same call. Report an outcome only after the corresponding application work is
actually committed. Never use a prompt, tool output, or credential as a milestone
message or metadata value.

For streaming APIs, fully consume the stream inside the Watchdog run. The Claude
example uses `contextlib.aclosing` so an early exit is observed before the run
closes. For LangGraph interrupts/checkpoints, keep the Watchdog run open through
the required resume, or instrument each execution segment as a separate job.
The adapter does not make persisted workflows into resumable Watchdog runs.
