# Watchdog Python SDK

Track the completion, progress, cost, tool activity, and expected outcomes of an
unattended job. The SDK sends metadata to the Watchdog API from a bounded background
queue. Monitoring outages do not raise exceptions into application code.

## Install and configure

```bash
python -m pip install watchdog-agent-sdk
export WATCHDOG_URL="https://withwatchdog.com"
export WATCHDOG_API_KEY="your-ingest-key"
```

The distribution is `watchdog-agent-sdk`; its import is `watchdog_agent`. This
avoids colliding with the unrelated Python filesystem package named `watchdog`.
The base SDK has no runtime dependencies and supports Python 3.10 or newer.

For local development, clone this repository and run `python -m pip install .`
from its root. Self-hosted installations can set `WATCHDOG_URL` to their API origin.

Register an agent and a job in the dashboard, then use the job ID or slug:

```python
from watchdog_agent import Watchdog

with Watchdog() as watchdog:
    with watchdog.run("daily-report") as run:
        with run.tool_call("competitors.search", arguments={"sector": "software"}):
            results = search_competitors()
        run.progress("Research complete", completed=len(results))

        report = create_report(results)
        run.outcome("report_created", metadata={"record_id": report.id})

        post_report(report)
        run.outcome("report_posted")
```

`run.started` is emitted on entry. Normal exit emits `run.completed`; an exception
emits `run.failed` and preserves the original exception. Cancellation emits
`run.cancelled`. Exceptions' type names are collected; their messages, stack
traces, and local variables are not collected. Completion means the agent exited;
the server evaluates whether the required business outcomes were satisfied.

Only explicit `run.progress()` calls indicate meaningful progress. Tool calls,
model calls, handoffs, and repeated activity do not reset the progress timer.

## Usage, costs, and outcomes

```python
run.usage(
    model="your-model-name",
    input_tokens=2100,
    output_tokens=300,
    cost_usd=0.017,
)
run.tool("crm.update", duration_ms=180, status="success", arguments={"id": record_id})
run.outcome("crm_updated", success=True, metadata={"record_id": record_id})
```

Each usage event is a **delta for one model request**, not the cumulative run
usage. Omit `cost_usd` when it is unknown. The SDK does not embed a stale model
price table. This version only sums explicitly reported costs; token counts alone do not calculate spending.
Report an outcome only after the corresponding application operation succeeds.
An emitted outcome is an application attestation, not independent verification.

Tool fingerprints use a per-run random HMAC key over canonical tool name,
arguments, and status. Raw arguments stay in the customer process. Reordered
JSON keys and insignificant JSON whitespace produce the same fingerprint;
different arguments normally produce different fingerprints. Without arguments,
only tool names/status distinguish calls. Emit explicit progress within legitimate
bulk-processing loops to avoid treating repeated work as a lack of progress.

## Async jobs and cancellation

```python
async with Watchdog() as watchdog:
    async with watchdog.run("daily-report", cancellable=True) as run:
        await execute_agent()
        run.outcome("report_created")
```

For a cancellable async context, an authenticated cancel command schedules
`Task.cancel()` on the task owning that context. It takes effect at an await
boundary. Application code must allow `asyncio.CancelledError` to propagate.
For synchronous code, call `run.check_cancelled()` between work units.
`run.tool_call()` checks automatically before entering a tool. Blocking external
operations cannot be forcefully stopped by this SDK.

The SDK acknowledges a cancel action only after the run exits cancelled. A
received command is not proof that execution has stopped. If the application
completes without observing cancellation, the action is acknowledged as failed.
Retry commands require a customer runtime webhook/controller; this client does
not retain business input or execute customer code again.

The included [signed control adapter](https://github.com/withwatchdog/watchdog-sdk/blob/main/examples/CONTROL_ADAPTER.md) implements a
customer-owned callback endpoint with cancellation, retry, fallback allowlisting,
missed-slot correlation, HMAC verification, and a durable action ledger. Its local
demo executes a supplied application function and writes report files.

Cancellation is out of band. Polling, network latency, in-flight tool requests,
and model requests can cause delay or budget overshoot. This is not a hard
real-time spend ceiling.

## Framework adapters

Release 0.2.0 adds Claude Agent SDK, LangGraph, and PydanticAI adapters and updates
the OpenAI Agents adapter. Install the optional extra for your framework:

```bash
python -m pip install 'watchdog-agent-sdk[openai]==0.2.0'
python -m pip install 'watchdog-agent-sdk[claude]==0.2.0'
python -m pip install 'watchdog-agent-sdk[langgraph]==0.2.0'
python -m pip install 'watchdog-agent-sdk[pydanticai]==0.2.0'
```

Install only the extras your application needs. Plain Python needs none.
The OpenAI extra updates its framework dependency; review upstream migration
notes if your application is pinned to the older 0.8.4 API. Watchdog's existing
`Watchdog`, `Run`, and `openai_agents.WatchdogHooks` entry points are preserved.

| Integration | Optional extra | Stable packages verified on 2026-09-06 | Official mechanism |
| --- | --- | --- | --- |
| Plain Python | None | Python 3.10+; no runtime dependencies | Sync/async `Run` context, `tool_call`, `progress`, `usage`, `outcome` |
| OpenAI Agents | `openai` | `openai-agents==0.22.0`, `openai==3.8.0` | `RunHooks` passed to `Runner` |
| Claude Agent SDK | `claude` | `claude-agent-sdk==0.2.152` | Tool hooks and `ResultMessage` in a finite query stream |
| LangGraph | `langgraph` | `langgraph==1.2.11`, `langchain-core==1.6.2` | `BaseCallbackHandler` through `RunnableConfig.callbacks` |
| PydanticAI | `pydanticai` | `pydantic-ai-slim==2.40.0` | Native `Capability` wrapping model/tool execution |

PydanticAI's official slim distribution provides `pydantic_ai` without installing
every model provider. Install the provider extras required by your own application
separately. No framework imports happen when importing `watchdog_agent`.

All adapters share the same bounded queue, privacy handling, event IDs, retry
policy, local tool fingerprints, and bounded timing state. They do not patch
framework internals or install a tracing exporter. The outer Watchdog run context
owns application completion/failure, including nested agents and handoffs.

### OpenAI Agents

```python
from agents import Runner, RunConfig
from watchdog_agent import Watchdog
from watchdog_agent.openai_agents import WatchdogHooks

async with Watchdog() as watchdog:
    async with watchdog.run("daily-report") as run:
        result = await Runner.run(
            agent,
            "Prepare the report",
            hooks=WatchdogHooks(run),
            run_config=RunConfig(trace_include_sensitive_data=False),
        )
        # Save the result in your application before reporting an outcome.
```

Uses native agent, handoff, model and function-tool hooks. Model starts measure
latency locally; model ends report per-response token deltas, not cumulative
context usage. Tool call IDs correlate parallel calls. A context without IDs
uses FIFO correlation, so timing can be approximate for concurrent same-name
tools. The current hooks have no error-end callback: escaping errors reach the
outer run context, but errors converted to tool result text cannot safely be
classified. Hosted tools that bypass function-tool hooks are not recorded.

OpenAI tracing is separate. The example restricts sensitive data in OpenAI
tracing; it does not modify user-supplied Watchdog messages or metadata.

### Claude Agent SDK

```python
from contextlib import aclosing
from claude_agent_sdk import ClaudeAgentOptions, query
from watchdog_agent import Watchdog
from watchdog_agent.claude_agents import WatchdogClaudeMonitor

async with Watchdog() as watchdog:
    async with watchdog.run("daily-report") as run:
        monitor = WatchdogClaudeMonitor(run)
        options = ClaudeAgentOptions(hooks=monitor.hooks(), max_turns=3)
        async with aclosing(monitor.stream(query(prompt="Prepare the report", options=options))) as stream:
            async for message in stream:
                pass  # Handle the original Claude messages in your application.
```

For existing options, use `options.hooks = monitor.hooks(options.hooks)` to append
observation hooks without replacing permission hooks. Watchdog returns an empty
hook response; it does not grant tool access or change results.

`PreToolUse`, `PostToolUse`, and `PostToolUseFailure` provide local timing,
fingerprints, and failure status. Reported error results mark the run failed even
if Claude raises no exception. Aborted results mark it cancelled. A stream that
ends without a result, or is explicitly closed before a result, marks it failed.
Closing the wrapper also closes the underlying query iterator when supported. Consume
the finite `query()` or `ClaudeSDKClient.receive_response()` stream inside the run
and use `aclosing` if consumption may stop early. Do not use an indefinitely open
`receive_messages()` stream as the boundary of one job.

Claude usage records cover each result's **main-agent turn**, not individual model
requests or subagents. Input counts include cache-read and cache-creation input tokens when reported.
They are labeled `usage_scope=main_agent_turn`.
Cumulative session `model_usage` and `total_cost_usd` are not summed into the run;
the adapter does not report cost or claim complete session accounting. There are
no individual model-start/end records from this adapter. Application exceptions
still propagate unchanged. A normal stream result is not proof of business
success: explicitly attest outcomes only after your work succeeds.

### LangGraph

```python
from watchdog_agent import Watchdog
from watchdog_agent.langgraph import WatchdogCallbackHandler

async with Watchdog() as watchdog:
    async with watchdog.run("daily-report") as run:
        result = await graph.ainvoke(
            inputs,
            config={"callbacks": [WatchdogCallbackHandler(run)]},
        )
```

The same handler works with `graph.invoke()`, `ainvoke()`, and fully consumed
graph streams. Append it to existing callbacks. For nested async model/tool
calls, explicitly pass the node's `RunnableConfig` onward, particularly on
Python 3.10. Plain functions that do not emit framework callbacks need explicit
`run.tool_call()` instrumentation.

Model/chat callbacks report canonical `AIMessage.usage_metadata`, falling back
to standard provider `llm_output.token_usage` when available. Tool/model errors
are recorded without exception messages. A `ToolMessage(status="error")` is a
reported failure; arbitrary result strings are never inspected for errors.

Graph/node completion does not end the outer run. An interrupt/checkpoint can
return control before the business job finishes: keep that run context open
through the required resume, or monitor individual execution segments. This
adapter does not persist/resume a Watchdog context across processes.

### PydanticAI

```python
from watchdog_agent import Watchdog
from watchdog_agent.pydantic_ai import WatchdogCapability

async with Watchdog() as watchdog:
    async with watchdog.run("daily-report") as run:
        result = await agent.run(
            "Prepare the report",
            capabilities=[WatchdogCapability(run)],
        )
```

A fresh per-run capability also works with `run_sync()` and fully consumed
`run_stream()`. Native `wrap_model_request` and `wrap_tool_execute` await the
original handler exactly once and return its result. They report elapsed time,
per-response usage, function-tool fingerprints, and raised failures, preserving
the original exception. Approval, deferral and skip control flow are not counted
as dependency errors. `ModelRetry` is a failed attempt, not meaningful progress.
Arguments rejected before execution and provider-hosted tools are outside the
tool-execution hook's coverage. If tools are deferred, keep the outer job open
until the application has resolved the work; the capability does not resume it.

### Coverage shared by all adapters

- Keep **progress monitoring disabled** for lifecycle-only jobs. Set a realistic
  runtime deadline and expected schedule in the dashboard.
- Only `run.progress()` resets the progress timer or marks a legitimate milestone.
  No automatic progress events come from model calls, tools, handoffs, or nodes.
- The hosted wire protocol supports `run.started`, `tool.completed`,
  `llm.completed`, `progress`, `outcome`, and terminal run events. Tool/model
  starts are used for local timing, not emitted as unsupported wire event types.
  Nested-agent starts/ends do not create independent Watchdog runs.
- Model failures use `llm.completed` with `metadata.status="error"`; unknown usage
  has `usage_available=false` and zero placeholders, not measured zero tokens.
  These records do not add a new server-side detector.
- Missing hooks and hard crashes can leave calls without completion records.
  Runtime/schedule detection remains server-side; the SDK cannot report a process
  death after it has been killed. Timing state is bounded to 1,024 outstanding
  starts per adapter; evictions increment the existing dropped counter.
- Prompts, completions, raw arguments, results and exception text are not sent.
  Names, counts, elapsed times, status, and exception type names are telemetry.
- Do not double-instrument the same tool with both a framework adapter and
  `run.tool_call()`. Where a framework hides error status, explicit instrumentation
  may be needed instead of recording that tool through the adapter.

The [0.2.0 source archive on PyPI](https://pypi.org/project/watchdog-agent-sdk/0.2.0/#files)
includes runnable examples for all five integrations in `examples/`, with setup
in `examples/README.md`. It also includes `ADAPTER_VERIFICATION.md` with official
references, checks and limitations. No private repository access is required.

## Delivery and privacy behavior

- The default queue holds 1,024 requests; enqueue never waits for network I/O.
  Full queues drop new telemetry and increment `watchdog.stats["dropped"]`.
- Events have unique IDs and per-run monotonic sequences. Retries preserve the
  original serialized event, timestamp, sequence, and ID for server deduplication.
- Transient failures receive two retries by default. HTTP 4xx other than 408/429
  are not retried. No prompts, outputs, payloads, or bearer keys are logged.
- Use the client as a context manager or call `watchdog.close(timeout=2)` before
  a short-lived process exits. `flush()` and `close()` return `False` if their
  bounded wait expires. Daemon threads cannot guarantee delivery after a crash
  or process kill. There is no durable disk spool in this version.
- Missing `WATCHDOG_API_KEY` disables delivery. Inspect `.enabled` and `.stats`
  during installation checks. Configuration errors, such as insecure remote URLs,
  raise immediately; network failures do not interrupt an otherwise valid job.
- Metadata accepts bounded JSON values and removes common secret-bearing keys.
  Application-provided messages, names, and metadata values are intentionally
  transmitted: do not place secrets in them. Secret-key filtering is defense in
  depth, not a content classifier.
- HTTPS is required except for loopback development. Redirects are rejected so
  bearer credentials are not forwarded to another host.
- Customer-supplied `headers={...}` can support additional deployment access
  layers. Authentication and request-framing headers cannot be overridden.

Create the client **after forking** a worker process. A client owns live threads
and is not process-safe. Reuse one client across jobs within each process.

## Wire protocol

`POST /api/v1/events` accepts a single JSON event with bearer authentication:

```json
{
  "event_id": "evt_a_unique_id",
  "run_id": "run_a_unique_id",
  "job_id": "daily-report",
  "type": "tool.completed",
  "sequence": 3,
  "timestamp": "2026-09-05T12:00:00.000Z",
  "data": {
    "name": "crm.update",
    "fingerprint": "a-per-run-hmac",
    "duration_ms": 180,
    "status": "success",
    "metadata": {}
  }
}
```

Types: `run.started`, `progress`, `tool.completed`, `llm.completed`, `outcome`,
`run.completed`, `run.failed`, `run.cancelled`.

`GET /api/v1/runs/{run_id}/commands` returns
`{"commands":[{"id":"action-id","type":"cancel"}]}`.
The SDK sends `POST /api/v1/actions/{action_id}/ack` with
`{"status":"acknowledged","message":"Runtime stopped cooperatively"}`
after cancellation takes effect, or `status: "failed"` when it cannot be applied.

## Tests

```bash
python -m pip install '.[openai,claude,langgraph,pydanticai,dev]'
python -m unittest discover -s tests -v
python -m ruff check watchdog_agent tests examples
python -m mypy
```

Run these commands from the cloned repository root. Tests cover outages,
queue saturation, bounded shutdown, retry idempotency,
privacy, concurrent event sequences, sync/async cancellation acknowledgments,
tool failures, outcomes, and all four native adapter mechanisms without requiring
a model API key. Framework tests are explicitly skipped when their extra is
absent. Install all extras for the full suite. Tests use in-memory telemetry
transports, official fake models, and Claude SDK message/hook fixtures; the
existing control-adapter HTTP test uses loopback only.

## Start with lifecycle-only monitoring

New Watchdog jobs leave progress monitoring disabled (`progress_timeout: 0`) and have no expected schedule unless you add one. Set a realistic maximum runtime, for example 1,200 seconds for a legitimate 15-minute job. The run context reports start, completion and failure; it cannot report a process kill. A missing completion is detected when the runtime deadline expires.

Add `run.progress()` after meaningful completed work, then explicitly enable a milestone timeout in the job's Additional checks. The timer starts at run start. Tool calls do not reset it, and emitting progress does not turn the timeout on. Progress can still reset the tool-loop window while milestone timeout monitoring is off. Existing jobs retain their saved policies; edits apply to future runs.

Install lifecycle-only monitoring without any framework dependencies:

```bash
python -m pip install watchdog-agent-sdk==0.2.0
```

Use `watchdog-agent-sdk`, not the unrelated `watchdog` filesystem package.
