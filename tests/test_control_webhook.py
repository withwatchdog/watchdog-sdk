import asyncio
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib.request import Request, urlopen

from examples.control_webhook import ActionLedger, ControlAdapter, ControlError, authenticate, create_http_server
from watchdog_agent import Watchdog


SECRET = "test-signing-secret-" + "x" * 32


def signed(command, timestamp=None):
    body = json.dumps(command, separators=(",", ":")).encode()
    timestamp = str(int(time.time()) if timestamp is None else timestamp)
    signature = hmac.new(SECRET.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return body, {"x-watchdog-timestamp": timestamp, "x-watchdog-signature": "v1=" + signature}


class SigningTests(unittest.TestCase):
    def test_exact_body_signature_and_case_insensitive_headers(self):
        body, headers = signed({"action_id": "one"})
        authenticate(SECRET.encode(), {key.upper(): value for key, value in headers.items()}, body)
        with self.assertRaises(ControlError) as error:
            authenticate(SECRET.encode(), headers, body + b" ")
        self.assertEqual(error.exception.status, 401)

    def test_stale_and_future_timestamps_are_rejected(self):
        for delta in (-301, 301):
            body, headers = signed({}, timestamp=1000 + delta)
            with self.assertRaises(ControlError):
                authenticate(SECRET.encode(), headers, body, now=1000)

    def test_duplicate_id_with_different_body_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = ActionLedger(str(Path(directory) / "actions.sqlite3"))
            ledger.claim("id", "digest-one")
            ledger.finish("id", {"accepted": True})
            with self.assertRaises(ControlError) as error:
                ledger.claim("id", "digest-two")
            self.assertEqual(error.exception.status, 409)
            ledger.close()

    def test_crash_during_dispatch_requires_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "actions.sqlite3")
            ledger = ActionLedger(path)
            ledger.claim("id", "digest")
            ledger.close()
            recovered = ActionLedger(path)
            with self.assertRaises(ControlError) as error:
                recovered.claim("id", "digest")
            self.assertIn("reconciliation", error.exception.message)
            recovered.close()


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / "actions.sqlite3")
        self.requests = []
        self.loads = []
        self.executions = []
        self.client = Watchdog("test", _transport=lambda method, path, body: self.requests.append((path, json.loads(body))) or {})

        def load_input(job_id, parent_run_id, scheduled_at):
            self.loads.append((job_id, parent_run_id, scheduled_at))
            return {"private_business_input": "kept-local"}

        async def execute(job_id, payload, run, fallback_model):
            self.executions.append((job_id, payload, fallback_model))
            await asyncio.Event().wait()

        self.factory = lambda: ControlAdapter(self.client, SECRET, self.path, load_input=load_input, execute=execute, allowed_jobs={"job"}, allowed_fallback_models={"customer-fallback-model"})
        self.adapter = self.factory()

    async def asyncTearDown(self):
        await self.adapter.close()
        await asyncio.to_thread(self.client.close)
        self.temp.cleanup()

    async def test_cancel_ack_follows_actual_cancelled_event(self):
        run_id = await self.adapter.start("job", {"local": True})
        active = self.adapter.active[run_id]
        body, headers = signed({"action_id": "stop-one", "command": "cancel", "job_id": "job", "run_id": run_id})
        status, response = await self.adapter.handle(body, headers)
        self.assertEqual(status, 202)
        self.assertTrue(response["accepted"])
        with self.assertRaises(asyncio.CancelledError):
            await active.task
        await asyncio.to_thread(self.client.flush)
        terminal_index = next(index for index, (path, event) in enumerate(self.requests) if event.get("type") == "run.cancelled")
        ack_index = next(index for index, (path, event) in enumerate(self.requests) if path.endswith("/stop-one/ack"))
        self.assertGreater(ack_index, terminal_index)
        self.assertEqual(self.requests[ack_index][1]["status"], "acknowledged")

    async def test_retry_deduplicates_and_preserves_ancestry_and_fallback(self):
        command = {"action_id": "retry-one", "command": "retry", "job_id": "job", "run_id": "parent-run", "root_run_id": "root-run", "retry_number": 2, "fallback_model": "customer-fallback-model"}
        body, headers = signed(command)
        first_status, first = await self.adapter.handle(body, headers)
        second_status, second = await self.adapter.handle(body, headers)
        self.assertEqual((first_status, second_status), (202, 202))
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(len(self.loads), 1)
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.executions[0][2], "customer-fallback-model")
        await asyncio.to_thread(self.client.flush)
        start = next(event for path, event in self.requests if event.get("type") == "run.started")
        self.assertEqual(start["data"]["metadata"]["parent_run_id"], "parent-run")
        self.assertEqual(start["data"]["metadata"]["action_id"], "retry-one")
        self.assertNotIn("kept-local", json.dumps(self.requests))

    async def test_missed_slot_retry_preserves_schedule_and_action_id(self):
        command = {"action_id": "retry-slot", "command": "retry", "job_id": "job", "run_id": None, "scheduled_at": "2026-09-05T12:00:00.000Z"}
        status, response = await self.adapter.handle(*signed(command))
        self.assertEqual(status, 202)
        await asyncio.to_thread(self.client.flush)
        metadata = self.requests[0][1]["data"]["metadata"]
        self.assertEqual(metadata["scheduled_at"], command["scheduled_at"])
        self.assertEqual(metadata["action_id"], command["action_id"])
        self.assertNotIn("parent_run_id", metadata)

    async def test_duplicate_after_restart_does_not_start_another_retry(self):
        body, headers = signed({"action_id": "retry-once", "command": "retry", "job_id": "job", "run_id": "parent"})
        first_status, first = await self.adapter.handle(body, headers)
        await self.adapter.close()
        self.adapter = self.factory()
        status, response = await self.adapter.handle(body, headers)
        self.assertEqual(status, 202)
        self.assertTrue(response["duplicate"])
        self.assertEqual(response["run_id"], first["run_id"])
        self.assertEqual(len(self.executions), 1)

    async def test_untrusted_signature_cannot_stop_a_customer_run(self):
        run_id = await self.adapter.start("job", {})
        body, headers = signed({"action_id": "fake", "command": "cancel", "job_id": "job", "run_id": run_id})
        headers["x-watchdog-signature"] = "v1=" + "0" * 64
        status, response = await self.adapter.handle(body, headers)
        self.assertEqual(status, 401)
        self.assertFalse(self.adapter.active[run_id].run.cancel_requested)

    async def test_missing_runtime_is_failed_not_reported_stopped(self):
        status, response = await self.adapter.handle(*signed({"action_id": "missing", "command": "cancel", "job_id": "job", "run_id": "absent"}))
        self.assertEqual(status, 202)
        self.assertFalse(response["accepted"])
        await asyncio.to_thread(self.client.flush)
        self.assertEqual(self.requests[-1][1]["status"], "failed")

    async def test_real_loopback_http_receives_and_deduplicates_signed_commands(self):
        server = create_http_server(self.adapter, asyncio.get_running_loop(), port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        body, headers = signed({"action_id": "http-retry", "command": "retry", "job_id": "job", "run_id": "parent"})
        request = Request("http://127.0.0.1:" + str(server.server_port) + "/watchdog/control", data=body, headers={**headers, "Content-Type": "application/json"}, method="POST")
        def send():
            with urlopen(request, timeout=2) as response:
                return response.status, json.loads(response.read())
        try:
            first_status, first = await asyncio.to_thread(send)
            second_status, second = await asyncio.to_thread(send)
            self.assertEqual((first_status, second_status), (202, 202))
            self.assertTrue(second["duplicate"])
            self.assertEqual(first["run_id"], second["run_id"])
            self.assertEqual(len(self.executions), 1)
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
