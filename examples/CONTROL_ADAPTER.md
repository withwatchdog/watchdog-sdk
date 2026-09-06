# Customer runtime control adapter

`control_webhook.py` is a runnable, standard-library example of a customer-owned
control endpoint. Watchdog sends commands; the customer owns application input,
execution code, credentials, and runtime lifecycle. The demonstration calls no
LLM and no external service other than the configured Watchdog telemetry API.

After installing the SDK, create the `daily-report` job, configure its cancellation
and retry callback to your HTTPS endpoint, and use the signing secret generated
in Watchdog Settings:

```bash
export WATCHDOG_URL="https://your-watchdog-api.example"
export WATCHDOG_API_KEY="your-ingestion-key"
export WATCHDOG_SIGNING_SECRET="your-settings-generated-signing-secret"
python examples/control_webhook.py
```

The demo binds `127.0.0.1:8090/watchdog/control`. A production TLS reverse proxy
must route the configured HTTPS callback to that loopback endpoint. The demo
starts one report and writes a real JSON artifact under `work/control-demo/reports`
after approximately 20 seconds. Cancel/retry controls operate on that local work.
There is no model fallback configured for the demo.

Replace these two callbacks to integrate your own runtime:

```python
def load_input(job_id, parent_run_id, scheduled_at):
    # Read the original immutable business input from your own durable store.
    return your_job_store.lookup(job_id, parent_run_id, scheduled_at)

async def execute(job_id, payload, run, fallback_model):
    # Apply only model choices permitted by your own allowlist.
    result = await your_agent(payload, model=fallback_model)
    saved = await your_application.persist(result)
    run.outcome("report_created", metadata={"record_id": saved.id})
```

`load_input` is a synchronous, local lookup and must return promptly. Use a
customer queue or durable workflow controller when input retrieval or dispatch
requires long external operations. `execute` is an async customer function and
must allow `asyncio.CancelledError` to propagate. Polling and signed callbacks can
both be enabled: repeated cancel action IDs are deduplicated by the run context.

## Exact signature contract

Watchdog sends:

- `x-watchdog-timestamp`: integer Unix seconds, encoded as ASCII.
- `x-watchdog-signature`: `v1=` followed by lowercase HMAC-SHA256 hex.
- Signed bytes: `timestamp + "." + exact_raw_UTF8_JSON_body`.

The receiver compares signatures in constant time and rejects timestamps more
than 300 seconds in the past or future. Do not parse/re-encode the JSON before
signature verification. Keep the application clock synchronized. Requests are
limited to 64 KiB. Signatures, raw commands, and customer exception messages are
not logged by the example server.

A command body has:

```json
{
  "action_id": "action-id",
  "command": "retry",
  "run_id": "original-run-id",
  "job_id": "daily-report",
  "reason": "runtime_exceeded",
  "parent_run_id": "original-run-id",
  "root_run_id": "first-run-id",
  "retry_number": 1,
  "fallback_model": null
}
```

A missed-slot retry uses `run_id: null` with a required `scheduled_at` ISO
timestamp. The adapter reads business input from the customer's trusted store;
the command never supplies executable code or the business payload.

## Delivery, execution, and acknowledgment

HTTP 202 means the command was received and durably deduplicated. It does not
claim that an agent has stopped or that a retry has finished.

For cancellation, the adapter requests cooperative task cancellation. The run
context emits `run.cancelled` on actual exit, then queues
`POST /api/v1/actions/{id}/ack` with `status: "acknowledged"`. FIFO delivery
preserves that order. If the run is not active, the action is acknowledged as
failed. Blocking operations cannot be forcefully terminated by this adapter.

For retry, the customer runtime starts a new run and emits `run.started` before
the action acknowledgment is queued. Metadata carries `parent_run_id`,
`root_run_id`, `retry_number`, and `action_id` when supplied. For a missed-slot
retry it carries `scheduled_at` and `action_id`. The retry acknowledgment confirms
that execution started; the new run's terminal state reports whether it succeeded.

If terminal/start telemetry is permanently lost, the Watchdog backend can reject
the acknowledgment because its required evidence is missing. A failed transport
must not be interpreted as confirmation that the action completed.

## Idempotency and restart behavior

The SQLite ledger commits an action ID and body digest before dispatch. Repeated
delivery of the same action returns its original response without starting
another retry. Reusing an action ID for a different body is rejected. Accepted
actions remain deduplicated after process restart.

A crash between the durable claim and successful dispatch leaves an uncertain
action. The example returns HTTP 409 for it until the operator reconciles it
with their customer runtime. It deliberately cannot promise atomic exactly-once
execution across SQLite and an arbitrary customer system. A production queue or
workflow engine should use `action_id` as its own durable idempotency key and
reconcile these uncertain actions against its execution records.

The example's active task registry is in memory and belongs to one process. A
process restart ends those tasks; use a durable customer runtime if jobs must
survive restarts. Do not run several adapter processes against the same SQLite
file: each adapter owns one runtime and one ledger. Back up the ledger and retain
its action IDs for the full retry/replay retention period.
