"""Local telemetry sink shared by adapter tests; never contacts Watchdog."""

import json
import unittest

from watchdog_agent import Watchdog


class CaptureCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sent = []
        self.client = Watchdog(
            "test",
            base_url="http://localhost:3000",
            _transport=lambda method, path, body: self.sent.append(json.loads(body)),
        )
        self.addCleanup(self.client.close)

    @property
    def events(self):
        self.assertTrue(self.client.flush())
        return self.sent

    def records(self, event_type):
        return [event["data"] for event in self.events if event["type"] == event_type]

    def assert_private(self):
        self.assertNotIn("private-", json.dumps(self.events))
        self.assertEqual(self.records("progress"), [])
