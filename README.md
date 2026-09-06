# Watchdog Python SDK

Track the completion, progress, cost, tool activity, and expected outcomes of an
unattended job. The SDK sends metadata to the Watchdog API from a bounded background
queue. Monitoring outages do not raise exceptions into application code.

## Install and configure

```bash
python -m pip install ./sdk
export WATCHDOG_URL="https://your-watchdog-api.example"
export WATCHDOG_API_KEY="your-ingest-key"
```

The distribution is `watchdog-agent-sdk`; its import is `watchdog_agent`. This
avoids colliding with the unrelated Python filesystem package named `watchdog`.
The base SDK has no runtime dependencies and supports Python 3.10 or newer.

Register an agent and a job in the dashboard, then use the job ID or slug:

```python
from watchdog_agent import Watchdog

with Watchdog() as watchdog:
    with watchdog.run("daily-report", cancellable=True) as run:
        with run.tool_call("competitors.search", arguments={"sector": "software"}):
            results = search_competitors()
        run.progress("Research complete", completed=len(results))

        report = create_report(results)
        run.outcome("report_created", metadata={"record_id": report.id})

        run.check_cancelled()
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

The included [signed control adapter](examples/CONTROL_ADAPTER.md) implements a
customer-owned callback endpoint with cancellation, retry, fallback allowlisting,
missed-slot correlation, HMAC verification, and a durable action ledger. Its local
demo executes a supplied application function and writes report files.

Cancellation is out of band. Polling, network latency, in-flight tool requests,
and model requests can cause delay or budget overshoot. This is not a hard
real-time spend ceiling.

## OpenAI Agents adapter

```bash
python -m pip install './sdk[openai]'
```

The optional extra pins the verified combination `openai-agents==0.8.4` and
`openai==2.19.0`. Run the integration suite when upgrading either dependency;
the upstream Agents version's broad dependency range also permits a newer,
incompatible usage schema. Details are in `ADAPTER_VERIFICATION.md`.

```python
from agents import Runner, RunConfig
from watchdog_agent.openai_agents import WatchdogHooks

async with watchdog.run("daily-report", cancellable=True) as run:
    result = await Runner.run(
        agent,
        business_input,
        hooks=WatchdogHooks(run),
        run_config=RunConfig(trace_include_sensitive_data=False),
    )
    # Save the result in your application before recording the outcome.
    saved = await save_report(result.final_output)
    run.outcome("report_created", metadata={"record_id": saved.id})
```

Use one hook instance per Watchdog run. The outer context owns job completion,
so handoffs and nested-agent completion cannot finish it prematurely. Per-response
token usage is used; cumulative `context.usage` would double-count earlier calls.
OpenAI's own tracing is separate from Watchdog telemetry: disabling sensitive
data there does not change application-provided Watchdog messages or metadata.

Adapter coverage and limitations:

- Lifecycle hooks capture function-tool completions and per-model response usage.
- Hosted provider tools may not pass through function-tool hooks; supply explicit
  instrumentation when that activity needs to count toward a tool-call policy.
- Tool exceptions converted by a framework into result strings cannot be
  classified safely without examining sensitive output. Use `run.tool_call()`
  inside a custom tool, or emit explicit error status for accurate error storms.
  Avoid also counting the same completed tool through the adapter.
- Call IDs distinguish concurrent tool calls where the framework exposes them.
  Older contexts without call IDs use a FIFO fallback; durations may be
  approximate for parallel calls of the same tool.
- Hook-generated events do not imply business outcomes or semantic progress.

The official [Agents SDK guide](https://developers.openai.com/api/docs/guides/agents)
and [running agents guide](https://developers.openai.com/api/docs/guides/agents/running-agents)
describe the SDK execution model. See `ADAPTER_VERIFICATION.md` for the exact
compatibility evidence and remaining limitations.

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
cd sdk
python -m unittest discover -s tests -v
```

Tests cover outages, queue saturation, bounded shutdown, retry idempotency,
privacy, concurrent event sequences, sync/async cancellation acknowledgments,
tool failures, outcomes, and hook mapping without requiring a model API key.

## Start with lifecycle-only monitoring

New Watchdog jobs leave progress monitoring disabled (`progress_timeout: 0`) and have no expected schedule unless you add one. Set a realistic maximum runtime, for example 1,200 seconds for a legitimate 15-minute job. The run context reports start, completion and failure; it cannot report a process kill. A missing completion is detected when the runtime deadline expires.

Add `run.progress()` after meaningful completed work, then explicitly enable a milestone timeout in the job's Additional checks. The timer starts at run start. Tool calls do not reset it, and emitting progress does not turn the timeout on. Progress can still reset the tool-loop window while milestone timeout monitoring is off. Existing jobs retain their saved policies; edits apply to future runs.

Install without repository access:

```sh
python -m pip install https://withwatchdog.com/downloads/watchdog_agent_sdk-0.1.0.tar.gz
```

This is Watchdog's source distribution, not the unrelated `watchdog` filesystem package.
