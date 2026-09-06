import asyncio
import json
import threading
import time
import unittest
from urllib.error import HTTPError, URLError

from watchdog_agent import Watchdog, WatchdogCancelled


class Transport:
    def __init__(self):
        self.requests = []
        self.lock = threading.Lock()
        self.failures = []
        self.commands = []

    def __call__(self, method, path, body):
        with self.lock:
            self.requests.append((method, path, json.loads(body) if body else None))
            if method == "GET":
                return {"commands": list(self.commands)}
            if self.failures:
                raise self.failures.pop(0)
        return {}

    @property
    def events(self):
        return [body for method, path, body in self.requests if path == "/api/v1/events"]

    @property
    def acks(self):
        return [body for method, path, body in self.requests if path.endswith("/ack")]


class SDKTests(unittest.TestCase):
    def client(self, transport=None, **kwargs):
        transport = transport or Transport()
        client = Watchdog("test-private-key", _transport=transport, **kwargs)
        self.addCleanup(client.close)
        return client, transport

    def test_success_events_are_ordered_and_prompt_free(self):
        client, transport = self.client()
        with client.run("daily-report") as run:
            run.progress("Research complete", completed=2, total=2)
            run.tool("search", arguments={"query": "secret customer details"})
            run.usage(model="customer-configured-model", input_tokens=10, output_tokens=5, cost_usd=0.03)
            run.outcome("report_created")
        self.assertTrue(client.flush())
        events = transport.events
        self.assertEqual([e["type"] for e in events], ["run.started", "progress", "tool.completed", "llm.completed", "outcome", "run.completed"])
        self.assertEqual([e["sequence"] for e in events], list(range(1, 7)))
        self.assertEqual(len({e["event_id"] for e in events}), 6)
        self.assertTrue(all(e["run_id"] == run.id for e in events))
        self.assertNotIn("secret customer", json.dumps(events))
        self.assertNotIn("test-private-key", repr(client))

    def test_application_error_propagates_without_its_sensitive_message(self):
        client, transport = self.client()
        original = ValueError("password=top-secret")
        with self.assertRaises(ValueError) as caught:
            with client.run("job"):
                raise original
        self.assertIs(caught.exception, original)
        client.flush()
        self.assertEqual(transport.events[-1]["type"], "run.failed")
        self.assertNotIn("top-secret", json.dumps(transport.events))
        self.assertEqual(transport.events[-1]["data"]["metadata"]["error_type"], "ValueError")

    def test_network_outage_is_fail_open(self):
        def failed(*args):
            raise URLError("network unavailable")
        client, _ = self.client(failed, retries=0)
        with client.run("job") as run:
            run.progress("Still working")
            result = 42
        self.assertEqual(result, 42)
        self.assertTrue(client.flush())
        self.assertEqual(client.stats["dropped"], 3)

    def test_retries_preserve_event_id_and_payload(self):
        client, transport = self.client(retries=1)
        transport.failures.append(HTTPError("url", 503, "unavailable", {}, None))
        with client.run("job"):
            pass
        client.flush()
        self.assertEqual(transport.events[0], transport.events[1])
        self.assertEqual(client.stats["retries"], 1)

    def test_permanent_authorization_error_is_not_retried(self):
        client, transport = self.client(retries=3)
        transport.failures.append(HTTPError("url", 401, "unauthorized", {}, None))
        with client.run("job"):
            pass
        client.flush()
        self.assertEqual(len(transport.events), 2)
        self.assertEqual(client.stats["retries"], 0)

    def test_queue_is_bounded_and_shutdown_does_not_hang(self):
        entered = threading.Event()
        release = threading.Event()
        def blocked(*args):
            entered.set()
            release.wait(1)
            return {}
        client, _ = self.client(blocked, queue_size=2)
        run = client.run("job").start()
        self.assertTrue(entered.wait(1))
        started = time.monotonic()
        for _ in range(100):
            run.progress("busy")
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertGreater(client.stats["dropped"], 0)
        self.assertFalse(client.close(timeout=0.01))
        release.set()
        self.assertTrue(client.flush())

    def test_metadata_snapshot_redacts_named_secrets_and_unsafe_objects(self):
        client, transport = self.client()
        metadata = {"count": 1, "nested": {"api_key": "never-send", "valid": 4}, "object": object()}
        with client.run("job") as run:
            run.progress("Step", metadata=metadata)
            metadata["count"] = 99
        client.flush()
        stored = transport.events[1]["data"]["metadata"]
        self.assertEqual(stored["count"], 1)
        self.assertEqual(stored["nested"], {"valid": 4})
        self.assertIsNone(stored["object"])
        self.assertNotIn("never-send", json.dumps(transport.events))

    def test_fingerprints_normalize_arguments_and_differ_between_runs(self):
        client, _ = self.client()
        one, two = client.run("job"), client.run("job")
        self.assertEqual(one.fingerprint("search", {"a": 1, "b": 2}), one.fingerprint("search", ' { "b": 2, "a": 1 } '))
        self.assertNotEqual(one.fingerprint("search", {"a": 1}), one.fingerprint("search", {"a": 2}))
        self.assertNotEqual(one.fingerprint("search", {"a": 1}), two.fingerprint("search", {"a": 1}))

    def test_multiple_threads_produce_monotonic_sequences(self):
        client, transport = self.client()
        with client.run("job") as run:
            workers = [threading.Thread(target=lambda: [run.progress("step") for _ in range(20)]) for _ in range(5)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
        client.flush()
        self.assertEqual([e["sequence"] for e in transport.events], list(range(1, 103)))

    def test_late_events_do_not_reopen_terminal_run(self):
        client, transport = self.client()
        with client.run("job") as run:
            pass
        self.assertFalse(run.progress("too late"))
        run.__exit__(None, None, None)
        client.flush()
        self.assertEqual(len(transport.events), 2)

    def test_cancellation_receipt_is_not_acknowledged_until_runtime_stops(self):
        client, transport = self.client()
        with self.assertRaises(WatchdogCancelled):
            with client.run("job", cancellable=True) as run:
                run._command({"id": "act-1", "type": "cancel"})
                run._command({"id": "act-1", "type": "cancel"})
                client.flush()
                self.assertEqual(transport.acks, [])
                run.check_cancelled()
        client.flush()
        self.assertEqual(transport.events[-1]["type"], "run.cancelled")
        self.assertEqual(len(transport.acks), 1)
        self.assertEqual(transport.acks[0]["status"], "acknowledged")
        self.assertTrue(transport.requests[-1][1].endswith("/act-1/ack"))

    def test_unhandled_sync_cancellation_is_reported_as_failed_action(self):
        client, transport = self.client()
        with client.run("job", cancellable=True) as run:
            run._command({"id": "act-1", "type": "cancel"})
        client.flush()
        self.assertEqual(transport.events[-1]["type"], "run.completed")
        self.assertEqual(transport.acks[-1]["status"], "failed")

    def test_tool_context_preserves_exception_and_marks_failure(self):
        client, transport = self.client()
        with self.assertRaises(KeyError):
            with client.run("job") as run:
                with run.tool_call("lookup", arguments={"record": 3}):
                    raise KeyError("private-customer")
        client.flush()
        tool = next(e for e in transport.events if e["type"] == "tool.completed")
        self.assertEqual(tool["data"]["status"], "error")
        self.assertNotIn("private-customer", json.dumps(transport.events))

    def test_missing_api_key_disables_network(self):
        transport = Transport()
        with Watchdog("", _transport=transport) as client:
            with client.run("job"):
                pass
        self.assertEqual(transport.requests, [])
        self.assertFalse(client.enabled)

    def test_unknown_cost_is_omitted(self):
        client, transport = self.client()
        with client.run("job") as run:
            run.usage(input_tokens=100, output_tokens=20)
        client.flush()
        self.assertNotIn("cost_usd", transport.events[1]["data"])

    def test_insecure_remote_endpoint_rejected(self):
        with self.assertRaises(ValueError):
            Watchdog("key", base_url="http://example.com")
        with self.assertRaises(ValueError):
            Watchdog("key", base_url="https://name:password@example.com")

    def test_http_transport_uses_bearer_key_and_preserves_optional_headers(self):
        client, _ = self.client(headers={"X-Customer-Access": "supplied-access"})
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self, limit):
                return b'{"accepted":true}'
        class Opener:
            def open(self, request, timeout):
                self.request, self.timeout = request, timeout
                return Response()
        opener = Opener()
        client._opener = opener
        result = client._http("POST", "/api/v1/events", b'{"test":true}')
        headers = {key.lower(): value for key, value in opener.request.header_items()}
        self.assertTrue(result["accepted"])
        self.assertEqual(headers["authorization"], "Bearer test-private-key")
        self.assertEqual(headers["x-customer-access"], "supplied-access")
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(opener.timeout, 2.0)

    def test_custom_headers_cannot_replace_authentication_or_inject_newlines(self):
        with self.assertRaises(ValueError):
            Watchdog("key", headers={"Authorization": "other-key"})
        with self.assertRaises(ValueError):
            Watchdog("key", headers={"X-Test": "value\r\nInjected: header"})

    def test_closed_client_does_not_apply_late_commands(self):
        client, _ = self.client()
        with client.run("job", cancellable=True) as run:
            client.close()
            run._command({"id": "late", "type": "cancel"})
            self.assertFalse(run.cancel_requested)

    def test_invalid_usage_values_are_dropped_instead_of_sending_nan_or_negative_spend(self):
        client, transport = self.client()
        with client.run("job") as run:
            self.assertFalse(run.usage(input_tokens=-1))
            self.assertFalse(run.usage(cost_usd=float("nan")))
            self.assertFalse(run.usage(cost_usd=-1))
        client.flush()
        self.assertEqual([e["type"] for e in transport.events], ["run.started", "run.completed"])


class AsyncSDKTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_poller_cancels_async_runtime_and_acks_after_exit(self):
        transport = Transport()
        client = Watchdog("test", _transport=transport, poll_interval=0.01)
        async def runtime():
            async with client.run("job", cancellable=True):
                transport.commands = [{"id": "async-stop", "type": "cancel"}]
                await asyncio.sleep(3)
        try:
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(runtime(), timeout=1)
            self.assertTrue(await asyncio.to_thread(client.flush))
            self.assertEqual(transport.events[-1]["type"], "run.cancelled")
            self.assertEqual(transport.acks[-1]["status"], "acknowledged")
        finally:
            client.close()

    async def test_normal_async_context_completes(self):
        transport = Transport()
        async with Watchdog("test", _transport=transport) as client:
            async with client.run("job") as run:
                await asyncio.sleep(0)
                run.outcome("record_saved")
        self.assertEqual(transport.events[-1]["type"], "run.completed")


if __name__ == "__main__":
    unittest.main()
