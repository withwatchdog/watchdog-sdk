"""Bounded, asynchronous, fail-open telemetry using only the Python standard library.

Network work happens on daemon threads. Runtime instrumentation only enqueues
immutable JSON. Call close() during graceful process shutdown to drain the queue.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
from itertools import islice
import json
import math
import os
import queue
import re
import secrets
import threading
import time
from typing import Any, Callable, Iterator, Literal
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid


_PRIVATE_KEY = re.compile(
    r"authorization|cookie|password|secret|api[_-]?key|access[_-]?token|"
    r"prompt|completion|tool[_-]?(arguments|args|result|output)|raw[_-]?(input|output)",
    re.IGNORECASE,
)
_MAX_BODY = 65536


def _safe(value: Any, depth: int = 0) -> Any:
    """Bound metadata; never call repr/str on arbitrary application objects."""
    if depth > 5:
        return None
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value[:1024]
    if isinstance(value, int):
        return value if abs(value) < 2**53 else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {
            key[:100]: _safe(item, depth + 1)
            for key, item in islice(value.items(), 50)
            if isinstance(key, str) and not _PRIVATE_KEY.search(key)
        }
    if isinstance(value, (list, tuple)):
        return [_safe(item, depth + 1) for item in value[:50]]
    return None


def _number(value: Any) -> int | float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isfinite(value) and value >= 0:
            return value
    return None


class WatchdogCancelled(Exception):
    """An authenticated cancellation was requested and reached a checkpoint."""


class _NoRedirect(HTTPRedirectHandler):
    # Do not forward bearer credentials to redirected origins or downgraded URLs.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class _Pending:
    method: str
    path: str
    body: bytes | None


class Watchdog:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        *,
        queue_size: int = 1024,
        timeout: float = 2.0,
        retries: int = 2,
        poll_interval: float = 2.0,
        max_cancellable_runs: int = 128,
        enabled: bool = True,
        headers: dict[str, str] | None = None,
        _transport: Callable[[str, str, bytes | None], Any] | None = None,
    ) -> None:
        self.base_url = (base_url or os.environ.get("WATCHDOG_URL", "http://localhost:3000")).rstrip("/")
        parsed = urlsplit(self.base_url)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("WATCHDOG_URL must be an HTTP(S) URL")
        if parsed.scheme == "http" and not local:
            raise ValueError("Remote Watchdog servers require HTTPS")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("WATCHDOG_URL cannot contain credentials, query, or fragment")
        if queue_size < 1 or timeout <= 0 or retries < 0 or poll_interval <= 0 or max_cancellable_runs < 1:
            raise ValueError("Invalid queue, timeout, retry, or polling configuration")
        self._api_key = api_key if api_key is not None else os.getenv("WATCHDOG_API_KEY", "")
        if not isinstance(self._api_key, str) or "\n" in self._api_key or "\r" in self._api_key:
            raise ValueError("Invalid Watchdog API key")
        self.enabled = bool(enabled and self._api_key)
        self._headers = dict(headers or {})
        if any(not isinstance(key, str) or not isinstance(value, str) or not key or "\n" in key + value or "\r" in key + value for key, value in self._headers.items()):
            raise ValueError("Custom headers must be strings without newlines")
        if any(key.lower() in {"authorization", "host", "content-type", "content-length"} for key in self._headers):
            raise ValueError("Custom headers cannot override authentication or request framing")
        self.timeout, self.retries = timeout, retries
        self.poll_interval, self.max_cancellable_runs = poll_interval, max_cancellable_runs
        self._queue: queue.Queue[_Pending] = queue.Queue(maxsize=queue_size)
        self._lock = threading.RLock()
        self._stats = dict(queued=0, sent=0, dropped=0, errors=0, retries=0)
        self._runs: dict[str, Run] = {}
        self._closed = False
        self._stop = threading.Event()
        self._opener = build_opener(_NoRedirect())
        self._transport = _transport or self._http
        self._worker: threading.Thread | None = None
        self._poller: threading.Thread | None = None
        if self.enabled:
            self._worker = threading.Thread(target=self._work, daemon=True, name="watchdog-events")
            self._worker.start()

    def __repr__(self) -> str:
        return f"Watchdog(base_url={self.base_url!r}, enabled={self.enabled})"

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def _count(self, key: str) -> None:
        with self._lock:
            self._stats[key] += 1

    def _http(self, method: str, path: str, body: bytes | None) -> Any:
        request = Request(
            self.base_url + path,
            data=body,
            method=method,
            headers={
                **self._headers,
                "Authorization": "Bearer " + self._api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "watchdog-agent-sdk/0.2.0",
            },
        )
        with self._opener.open(request, timeout=self.timeout) as response:
            raw = response.read(_MAX_BODY + 1)
            if len(raw) > _MAX_BODY:
                raise ValueError("Watchdog response exceeded limit")
            return json.loads(raw) if raw else {}

    def _enqueue(self, method: str, path: str, payload: Any) -> bool:
        try:
            raw = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
            if len(raw) > _MAX_BODY:
                self._count("dropped")
                return False
            with self._lock:
                if not self.enabled or self._closed:
                    self._stats["dropped"] += 1
                    return False
                self._queue.put_nowait(_Pending(method, path, raw))
                self._stats["queued"] += 1
            return True
        except (queue.Full, TypeError, ValueError, OverflowError, RecursionError):
            self._count("dropped")
            return False

    def _work(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                pending = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                for attempt in range(self.retries + 1):
                    try:
                        self._transport(pending.method, pending.path, pending.body)
                        self._count("sent")
                        break
                    except Exception as error:
                        retryable = not isinstance(error, HTTPError) or error.code in {408, 429} or error.code >= 500
                        self._count("errors")
                        if not retryable or attempt >= self.retries:
                            self._count("dropped")
                            break
                        self._count("retries")
                        self._stop.wait(min(0.1 * (2**attempt), 2.0))
            finally:
                self._queue.task_done()

    def run(self, job: str, *, run_id: str | None = None, cancellable: bool = False, metadata: dict | None = None) -> Run:
        return Run(self, job, run_id=run_id, cancellable=cancellable, metadata=metadata)

    def _register(self, run: Run) -> None:
        if not self.enabled or not run.cancellable:
            return
        with self._lock:
            if self._closed or len(self._runs) >= self.max_cancellable_runs:
                return
            self._runs[run.id] = run
            if self._poller is None:
                self._poller = threading.Thread(target=self._poll, daemon=True, name="watchdog-commands")
                self._poller.start()

    def _unregister(self, run: Run) -> None:
        with self._lock:
            self._runs.pop(run.id, None)

    def _poll(self) -> None:
        while not self._stop.wait(self.poll_interval):
            with self._lock:
                runs = list(self._runs.values())
            for run in runs:
                if self._stop.is_set():
                    return
                try:
                    result = self._transport("GET", "/api/v1/runs/" + quote(run.id, safe="") + "/commands", None)
                    commands = result.get("commands", []) if isinstance(result, dict) else []
                    if isinstance(commands, list):
                        for command in commands[:100]:
                            if isinstance(command, dict):
                                run._command(command)
                except Exception:
                    self._count("errors")

    def _ack(self, action_id: str, status: str, message: str) -> None:
        self._enqueue("POST", "/api/v1/actions/" + quote(action_id, safe="") + "/ack", {"status": status, "message": message})

    def flush(self, timeout: float = 2.0) -> bool:
        """Wait at most timeout seconds for queued delivery; False means incomplete."""
        deadline = time.monotonic() + max(timeout, 0)
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

    def close(self, timeout: float = 2.0) -> bool:
        """Stop accepting new events and drain best effort within a bounded wait."""
        with self._lock:
            self._closed = True
            self._runs.clear()
        self._stop.set()
        return self.flush(timeout)

    def __enter__(self) -> Watchdog:
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        self.close()
        return False

    async def __aenter__(self) -> Watchdog:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> Literal[False]:
        await asyncio.to_thread(self.close)
        return False


class Run:
    def __init__(self, client: Watchdog, job: str, *, run_id: str | None, cancellable: bool, metadata: dict | None) -> None:
        if not isinstance(job, str) or not job or len(job) > 200:
            raise ValueError("job must be a nonempty job ID or slug")
        self.client, self.job = client, job
        self.id = run_id or "run_" + uuid.uuid4().hex
        if not isinstance(self.id, str) or not self.id or len(self.id) > 200:
            raise ValueError("run_id must be a string of at most 200 characters")
        self.cancellable = cancellable
        self._metadata = _safe(metadata or {})
        self._lock = threading.RLock()
        self._sequence = 0
        self._started = False
        self._finished = False
        self._started_at = 0.0
        self._cancelled = threading.Event()
        self._actions: dict[str, str] = {}
        self._secret = secrets.token_bytes(32)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None

    def _event(self, event_type: str, data: dict | None = None) -> bool:
        try:
            with self._lock:
                if not self._started or self._finished:
                    return False
                self._sequence += 1
                return self.client._enqueue("POST", "/api/v1/events", {
                    "event_id": "evt_" + uuid.uuid4().hex,
                    "run_id": self.id,
                    "job_id": self.job,
                    "type": event_type,
                    "sequence": self._sequence,
                    "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    "data": _safe(data or {}),
                })
        except Exception:
            self.client._count("dropped")
            return False

    def start(self) -> Run:
        with self._lock:
            if self._started:
                raise RuntimeError("A Watchdog Run may only start once")
            self._started = True
            self._started_at = time.monotonic()
            self._event("run.started", {"metadata": {**self._metadata, "cancellable": self.cancellable}})
        self.client._register(self)
        return self

    def progress(self, message: str = "", *, completed: int | None = None, total: int | None = None, metadata: dict | None = None) -> bool:
        details = dict(metadata or {})
        if completed is not None:
            details["completed"] = completed
        if total is not None:
            details["total"] = total
        return self._event("progress", {"message": message, "metadata": details})

    def fingerprint(self, name: str, arguments: Any = None, status: str = "success") -> str | None:
        """HMAC canonical arguments locally. No raw arguments leave the process."""
        try:
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except ValueError:
                    arguments = arguments.strip()
            serialized = json.dumps([name, arguments, status], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            return hmac.new(self._secret, serialized, hashlib.sha256).hexdigest()
        except (TypeError, ValueError, RecursionError, OverflowError):
            return None

    def tool(self, name: str, *, duration_ms: float = 0, status: str = "success", fingerprint: str | None = None, arguments: Any = None, metadata: dict | None = None) -> bool:
        return self._event("tool.completed", {
            "name": name,
            "duration_ms": _number(duration_ms),
            "status": status,
            "fingerprint": fingerprint or self.fingerprint(name, arguments, status),
            "metadata": metadata or {},
        })

    @contextmanager
    def tool_call(self, name: str, *, arguments: Any = None) -> Iterator[None]:
        self.check_cancelled()
        started = time.monotonic()
        try:
            yield
        except BaseException:
            self.tool(name, duration_ms=(time.monotonic() - started) * 1000, status="error", arguments=arguments)
            raise
        else:
            self.tool(name, duration_ms=(time.monotonic() - started) * 1000, arguments=arguments)

    def usage(self, *, model: str | None = None, input_tokens: int = 0, output_tokens: int = 0, cost_usd: float | None = None, metadata: dict | None = None) -> bool:
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in (input_tokens, output_tokens)):
            self.client._count("dropped")
            return False
        data = {"input_tokens": input_tokens, "output_tokens": output_tokens, "metadata": metadata or {}}
        if model is not None:
            data["model"] = model
        # Omitted cost is unknown, not zero; token counts alone do not calculate spending.
        if cost_usd is not None:
            value = _number(cost_usd)
            if value is None:
                self.client._count("dropped")
                return False
            data["cost_usd"] = value
        return self._event("llm.completed", data)

    def outcome(self, name: str, *, success: bool = True, metadata: dict | None = None) -> bool:
        return self._event("outcome", {"name": name, "status": "success" if success else "failed", "metadata": metadata or {}})

    @property
    def cancel_requested(self) -> bool:
        return self._cancelled.is_set()

    def check_cancelled(self) -> None:
        if self.cancel_requested:
            raise WatchdogCancelled("Cancellation requested by Watchdog")

    def _command(self, command: dict) -> None:
        if self.client._closed or not self.cancellable or command.get("run_id", self.id) != self.id:
            return
        action_id = command.get("id") or command.get("action_id")
        kind = command.get("type") or command.get("command") or command.get("action_type") or ""
        if not isinstance(action_id, str) or not action_id or len(action_id) > 200:
            return
        with self._lock:
            if action_id in self._actions or len(self._actions) >= 100:
                return
            if self._finished:
                return
            self._actions[action_id] = kind
            if kind not in {"cancel", "stop"}:
                self.client._ack(action_id, "failed", "This SDK supports cooperative cancellation only; configure a runtime webhook for retry.")
                return
            self._cancelled.set()
            if self._loop is not None and self._task is not None:
                def cancel_if_running() -> None:
                    with self._lock:
                        if not self._finished and self._task is not None and not self._task.done():
                            self._task.cancel()
                try:
                    self._loop.call_soon_threadsafe(cancel_if_running)
                except RuntimeError:
                    pass

    def _finish(self, event_type: str, data: dict | None = None) -> None:
        with self._lock:
            if self._finished:
                return
            self._event(event_type, {"duration_ms": (time.monotonic() - self._started_at) * 1000, **(data or {})})
            self._finished = True
            for action_id, kind in self._actions.items():
                if kind in {"cancel", "stop"}:
                    stopped = event_type == "run.cancelled"
                    self.client._ack(action_id, "acknowledged" if stopped else "failed", "Runtime stopped cooperatively" if stopped else "Run ended before cancellation took effect")
        self.client._unregister(self)

    def __enter__(self) -> Run:
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        if exc_type is None:
            self._finish("run.completed")
        elif issubclass(exc_type, (WatchdogCancelled, asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            self._finish("run.cancelled")
        else:
            self._finish("run.failed", {"metadata": {"error_type": exc_type.__name__}})
        return False

    async def __aenter__(self) -> Run:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        return self.start()

    async def __aexit__(self, exc_type, exc, tb) -> Literal[False]:
        return self.__exit__(exc_type, exc, tb)
