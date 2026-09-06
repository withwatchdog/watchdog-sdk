# OpenAI Agents adapter verification

The adapter uses the lifecycle contract below. Signatures include `self` and a
trailing `**kwargs` in the Watchdog implementation for forward-compatible keyword
extensions. Inputs are accepted only to match the framework contract and are not
serialized.

```python
async def on_agent_start(self, context, agent): ...
async def on_agent_end(self, context, agent, output): ...
async def on_handoff(self, context, from_agent, to_agent): ...
async def on_llm_start(self, context, agent, system_prompt, input_items): ...
async def on_llm_end(self, context, agent, response): ...
async def on_tool_start(self, context, agent, tool): ...
async def on_tool_end(self, context, agent, tool, result): ...
```

The adapter reads `response.usage.input_tokens` and
`response.usage.output_tokens`. It does not read response output or cumulative
run-context usage. For function calls it reads tool names, local argument
fingerprints, and elapsed monotonic time. If exposed by the framework,
`context.tool_call_id` is used only for local timing correlation and
`context.tool_arguments` is HMACed locally.

Sources inspected on September 5, 2026:

- [Official Agents SDK overview](https://developers.openai.com/api/docs/guides/agents)
- [Official running agents guide](https://developers.openai.com/api/docs/guides/agents/running-agents)
- [Official integrations and observability guide](https://developers.openai.com/api/docs/guides/agents/integrations-observability)

The official `openai-agents` 0.8.4 distribution was downloaded and its source
inspected. Wheel SHA-256:
`2383c6e8e59ed4146b89d1b6f53e34e55caf94bc14ae3fd704e7aad5021f4ff1`.

The optional dependency pins `openai-agents==0.8.4` and `openai==2.19.0` as a
compatible baseline. Resolving the upstream Agents dependency without this pin
installed `openai==2.54.0`, whose required `cache_write_tokens` usage field breaks
the Agents 0.8.4 default `Usage()` constructor before lifecycle hooks run. The
adapter does not monkeypatch upstream models or hide this compatibility failure.

Verified directly in the distribution:

- `agents/lifecycle.py`: all seven signatures above match. `RunHooks` is an alias
  for `RunHooksBase[TContext, Agent]`.
- `agents/run_internal/tool_execution.py`: function-tool hooks receive the
  `ToolContext`, including the call ID and arguments used by this adapter.
- `agents/tool_context.py`: `tool_call_id` and `tool_arguments` are required
  context fields. The adapter only hashes arguments locally.
- `agents/run_internal/run_loop.py`: per-response usage is added to the cumulative
  context before the end hook. Reading `response.usage` avoids double counting.
- `agents/run_internal/tool_actions.py`: some local action tools receive a plain
  run context, so per-call ID/argument fidelity is not universally available.

Four tests in `tests/test_real_openai.py` passed against the actual installed
Runner using a deterministic local Model. They verify exact installed signatures,
parallel tool calls with distinct private fingerprints, correct per-response token
counts, preserved Runner exceptions, and cooperative cancellation. The tested
environment used Python 3.12, `openai-agents==0.8.4`, `openai==2.19.0`, and
`pydantic==2.13.5`. No live model request was made.

Run the complete SDK and control-adapter suite after installing the optional
extra: `python -m unittest discover -s tests -v`. Without the optional dependency,
the four real Runner tests are explicitly skipped. The rest of the suite covers
fail-open delivery, privacy, idempotency, and signed control dispatch.

Final integration validation: all 39 tests passed, including the real loopback signed HTTP server test and four real OpenAI Runner tests.
