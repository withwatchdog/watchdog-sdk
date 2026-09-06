# Framework adapter verification — SDK 0.2.0

Verified on **2026-09-06**, macOS arm64, using Python **3.12.14** and
**3.10.20**. This report accompanies SDK release **0.2.0**; it records the
implementation and packaging checks performed for this version.

## Official contracts and versions

Official documentation and the matching installed distribution source were
inspected before implementation. Stable release metadata was checked on
2026-09-06; optional extras pin the combinations below so future upstream
changes do not silently change the verified contract.

| Integration | Packages | Public interface used | Official references |
| --- | --- | --- | --- |
| Plain Python | Standard library; no runtime dependencies | Existing sync/async `Watchdog.run()`, `Run.tool_call()`, `Run.usage()`, `Run.progress()`, `Run.outcome()` | [Python context managers](https://docs.python.org/3/library/contextlib.html) |
| OpenAI Agents | `openai-agents 0.22.0`, `openai 3.8.0` | `RunHooks`: agent start/end, handoff, LLM start/end, tool start/end | [Official overview](https://developers.openai.com/api/docs/guides/agents), [lifecycle API](https://openai.github.io/openai-agents-python/ref/lifecycle/) |
| Claude Agent SDK | `claude-agent-sdk 0.2.152` | `HookMatcher`, `PreToolUse`, `PostToolUse`, `PostToolUseFailure`; typed `ResultMessage` stream | [Tool hooks](https://code.claude.com/docs/en/agent-sdk/hooks), [Python reference](https://code.claude.com/docs/en/agent-sdk/python) |
| LangGraph | `langgraph 1.2.11`, `langchain-core 1.6.2` | `BaseCallbackHandler` passed through `RunnableConfig.callbacks`; model/chat/tool callbacks | [Streaming and callback propagation](https://docs.langchain.com/oss/python/langgraph/streaming), [callback reference](https://reference.langchain.com/python/langchain-core/callbacks) |
| PydanticAI | `pydantic-ai-slim 2.40.0` (same core API as `pydantic-ai 2.40.0`) | `Capability.wrap_model_request`, `Capability.wrap_tool_execute`, supplied through per-run `capabilities` | [Official hooks](https://pydantic.dev/docs/ai/core-concepts/hooks/), [capability reference](https://pydantic.dev/docs/ai/api/pydantic-ai/capabilities/) |

No deprecated APIs, prerelease releases, monkey-patching, full tracing exporter,
or framework dependency in the base installation was introduced. The pre-existing
OpenAI extra changes from 0.8.4/2.19.0 to the current stable framework/client pair.
Applications upgrading that extra must review upstream compatibility for their
own agent code. The existing Watchdog public entry points remain available.

## Architecture

The application owns the outer `Run` context. That context reports one start and
one terminal event, preserving exceptions and cancellation. Nested agent or graph
completion never ends the containing job.

Four thin adapters feed `watchdog_agent/_adapter.py`. This private helper owns
bounded, thread-safe timing correlation and completion normalization; it delegates
delivery, fingerprints, event IDs, retries, redaction and terminal state to the
existing core. It retains only names, local correlation keys, timestamps and HMAC
fingerprints. A shared budget limits each adapter to 1,024 outstanding starts.
No raw prompts, arguments, results or exception messages are retained by it.

No automatic progress events are generated. Milestones remain explicit.
The wire protocol is unchanged: starts of tools/models are timed locally;
completion/error observations use existing `tool.completed` and `llm.completed`
records. Model-error status is metadata, not a new monitoring detector.
No backend migration, new endpoint, service or secret is required.

## Executed checks

All rows below describe executed checks, not inferred passes from source edits.
Test logs are local under `work/sdk-frameworks/` in the parent workspace.

| Check | Actual result on 2026-09-06 | Relevant files |
| --- | --- | --- |
| Full suite, Python 3.12.14 | **PASS: 74 tests, 0 failures/errors/skips** | `tests/test_*.py` |
| Same suite, Python 3.10.20 | **PASS: 74 tests, 0 failures/errors/skips**, using the installed wheel outside the SDK checkout | `tests/test_*.py` |
| External-network guard | **PASS: 74 tests, 0 external network attempts**, with provider keys removed; loopback allowed for the existing control-server test | All tests; local `offline-test.log` |
| Base wheel, new Python 3.12 environment | **PASS: 41 tests executed; 33 framework tests explicitly skipped**; loaded SDK from site-packages with no framework packages installed | `tests/test_adapter_events.py`, core/control tests |
| Independent extras | **PASS**: Claude-only and PydanticAI-only fresh installations import and run their adapter suites without the other frameworks | Optional extras and adapter tests |
| Lint | **PASS**, Ruff 0.16.6, configured errors/undefined-name checks | `pyproject.toml`, SDK/tests/examples |
| Types | **PASS**, mypy 2.3.1, all 7 SDK source files, Python 3.10 target, untyped function bodies checked | `pyproject.toml`, `watchdog_agent/*.py` |
| Distribution | **PASS**, wheel and sdist build; strict Twine metadata checks; no bytecode/private files; base dependencies empty; `py.typed` included | `pyproject.toml`, `MANIFEST.in` |
| Local examples | **PASS**: plain Python, LangGraph and PydanticAI examples executed with telemetry disabled | `examples/plain_python.py`, `examples/langgraph_agent.py`, `examples/pydantic_ai_agent.py` |
| Live OpenAI / Claude calls | **NOT RUN**: intentionally no external provider request or provider credential used | `examples/openai_agent.py`, `examples/claude_agent.py` |
| Hosted ingestion and incident delivery | **NOT RUN**: this task verifies SDK behavior and packaging; no production job or alert was created | Existing wire protocol retained |

The 35 added tests cover:

- Eight shared/core checks: lifecycle-only behavior, pending-state bounds,
  concurrent correlation, error fingerprints, fail-open delivery, unknown usage,
  separate-run isolation, and imports with optional dependencies blocked.
- Eleven Claude checks using actual SDK message types, options and hook matchers:
  permission-hook preservation, parallel tool success/failure, privacy,
  per-turn usage versus cumulative session counters, cached input tokens,
  reported errors, cancelled results, incomplete streams, explicit early close,
  original exceptions, malformed usage, source cleanup, and closing after a terminal
  result without creating a false failure. Some tests cover multiple cases.
- Seven LangGraph checks including real sync/async graphs, parallel nested
  callbacks, real tool exceptions, reported `ToolMessage` errors, model failures,
  usage fallbacks, n-best deduplication and malformed usage.
- Nine PydanticAI checks including real sync/async agents, parallel tools,
  streamed final usage, real model/tool errors, approval/deferral control flow,
  retry attempts, cancellation and instrumentation failure.
- The existing six OpenAI tests still pass against the new stable Runner,
  including exact lifecycle signatures, privacy, parallel calls, per-response
  usage, preserved exceptions and cooperative cancellation.

Local example outputs:

```text
plain_python.py:       [0, 2, 4]
langgraph_agent.py:    Synthetic report ready.
pydantic_ai_agent.py:  {"lookup":"Example record"}
```

## Reproduce

From this repository:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install '.[openai,claude,langgraph,pydanticai,dev]'
python -m unittest discover -s tests -v
python -m ruff check watchdog_agent tests examples
python -m mypy
python -m build
python -m twine check --strict dist/watchdog_agent_sdk-0.2.0*
```

The test suite never requires provider credentials. The OpenAI tests use a local
`Model` and disable native tracing. LangGraph uses its official fake chat model
and real tools/graphs. PydanticAI uses official `TestModel`/`FunctionModel`.
Claude tests construct official messages and invoke registered callbacks; they
do not start Claude's bundled CLI or a paid query. Telemetry uses local injected
transports, apart from the existing loopback control-server test.

For a clean-install check, install only the built wheel into a new environment.
Copy `tests/` and `examples/` to a temporary directory and run discovery there
without copying `watchdog_agent/`. This ensures imports exercise the wheel.

## Coverage limits

- OpenAI's current `RunHooks` has no dedicated model/tool error-end hook. The
  outer context records escaping failures. Errors converted to text and hosted
  tools that bypass function-tool hooks are not classified by content.
- Claude has no per-model request records here. Each result produces main-agent
  turn usage, labeled `usage_scope=main_agent_turn`; cache read/write input tokens
  are included when reported. Subagent/auxiliary usage and cumulative session
  cost are excluded, avoiding double counting after multi-turn/resumed sessions.
- LangGraph requires callback propagation into nested work. An interrupt is not
  the business job finishing: keep the outer context open through resume or
  monitor execution segments. There is no cross-process context persistence.
- PydanticAI observes executed function tools. Validation failures before
  execution and provider-hosted tools do not enter that hook. Approvals,
  deferrals and skips are control flow; the customer still owns resuming work.
- All streamed work must be consumed inside the outer run. Use `aclosing` for
  Claude when an early exit is possible. Hard process death cannot send a final
  event; server-side runtime/schedule checks remain necessary.
- No adapter infers semantic progress, outcomes, model prices, universal tool
  coverage, or zero false positives. Explicit progress/outcomes remain customer
  attestations. Instrument each operation once to avoid duplicate tool counts.

## Release and rollback

Release version: **0.2.0**. The distribution includes the wheel and source archive.
There are no new Watchdog environment variables or secrets. The usual
`WATCHDOG_URL` and `WATCHDOG_API_KEY` still configure telemetry; model-provider
credentials belong to the application's chosen provider setup.

Install with `python -m pip install watchdog-agent-sdk==0.2.0`, adding an optional
extra for your framework as documented in the README. No cloud deployment is
required for these adapters.
To roll back an application, remove new adapter imports/configuration and pin
`watchdog-agent-sdk==0.1.0` (and, if used, its original OpenAI extra). Existing
Watchdog jobs, keys and server data need no migration or rollback.

## Files changed

- Core and adapters: `watchdog_agent/client.py`, `watchdog_agent/__init__.py`,
  `watchdog_agent/_adapter.py`, `watchdog_agent/openai_agents.py`,
  `watchdog_agent/claude_agents.py`, `watchdog_agent/langgraph.py`,
  `watchdog_agent/pydantic_ai.py`, `watchdog_agent/py.typed`.
- Packaging and checks: `pyproject.toml` (optional extras, version, development
  checks and typing marker). Base runtime dependencies remain empty.
- Documentation: `README.md`, `ADAPTER_VERIFICATION.md`, `examples/README.md`.
- Examples: `examples/plain_python.py`, `examples/openai_agent.py`,
  `examples/claude_agent.py`, `examples/langgraph_agent.py`,
  `examples/pydantic_ai_agent.py`.
- Tests: `tests/_support.py`, `tests/test_adapter_events.py`,
  `tests/test_openai_adapter.py`, `tests/test_claude_adapter.py`,
  `tests/test_langgraph_adapter.py`, `tests/test_pydantic_ai_adapter.py`.

Core changes are limited to version reporting, typing corrections and an unused
import. Existing delivery/retry/cancellation behavior and the wire schema remain
in place. The tracked bytecode that predated this change was left unchanged.
