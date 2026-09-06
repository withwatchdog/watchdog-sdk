"""Shared, bounded observation state. No framework imports or application payloads."""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Hashable
from dataclasses import dataclass
import threading
import time
from typing import Any

from .client import Run


@dataclass(frozen=True)
class _Span:
    started: float
    name: str | None
    fingerprint: str | None = None
    error_fingerprint: str | None = None


class AdapterEvents:
    """Starts measure duration locally; only supported completion events are sent.

    Frameworks without call IDs use FIFO correlation. All outstanding starts,
    including repeated starts of the same tool, share one fixed memory budget.
    """

    def __init__(self, run: Run, framework: str) -> None:
        self.run, self.framework = run, framework
        self._pending: OrderedDict[tuple[str, Hashable], deque[_Span]] = OrderedDict()
        self._count = 0
        self._lock = threading.Lock()
        self._limit = 1024

    def start(self, kind: str, key: Hashable, name: str | None = None, arguments: Any = None) -> None:
        try:
            name = name if isinstance(name, str) else None
            span = _Span(
                time.monotonic(),
                name,
                self.run.fingerprint(name or "tool", arguments) if kind == "tool" else None,
                self.run.fingerprint(name or "tool", arguments, "error") if kind == "tool" else None,
            )
            with self._lock:
                if self._count >= self._limit:
                    oldest = next(iter(self._pending))
                    self._pop(*oldest)
                    self.run.client._count("dropped")
                self._pending.setdefault((kind, key), deque()).append(span)
                self._count += 1
        except Exception:
            self.run.client._count("dropped")

    def _pop(self, kind: str, key: Hashable) -> _Span | None:
        entries = self._pending.get((kind, key))
        if not entries:
            return None
        span = entries.popleft()
        self._count -= 1
        if not entries:
            del self._pending[(kind, key)]
        return span

    def discard(self, kind: str, key: Hashable) -> None:
        with self._lock:
            self._pop(kind, key)

    def _finish(self, kind: str, key: Hashable, error: BaseException | None) -> tuple[_Span | None, dict[str, Any]]:
        with self._lock:
            span = self._pop(kind, key)
        metadata: dict[str, Any] = {"framework": self.framework}
        if span is not None:
            metadata["duration_ms"] = max(0, (time.monotonic() - span.started) * 1000)
        if error is not None:
            metadata["error_type"] = type(error).__name__
        return span, metadata

    def tool_end(
        self, key: Hashable, *, name: str = "tool", failed: bool = False, error: BaseException | None = None
    ) -> None:
        try:
            span, metadata = self._finish("tool", key, error)
            failed = failed or error is not None
            self.run.tool(
                (span.name if span else None) or name,
                duration_ms=metadata.pop("duration_ms", 0),
                status="error" if failed else "success",
                fingerprint=(span.error_fingerprint if failed else span.fingerprint) if span else None,
                metadata=metadata,
            )
        except Exception:
            self.run.client._count("dropped")

    def model_end(
        self,
        key: Hashable,
        *,
        model: str | None = None,
        usage: Any = None,
        error: BaseException | None = None,
        failed: bool = False,
        scope: str = "request",
    ) -> None:
        try:
            span, metadata = self._finish("model", key, error)
            metadata.update(
                status="error" if failed or error is not None else "success",
                usage_available=usage is not None,
                usage_scope=scope,
            )
            read = usage.get if isinstance(usage, dict) else lambda field, default: getattr(usage, field, default)
            self.run.usage(
                model=model if isinstance(model, str) else span.name if span else None,
                input_tokens=read("input_tokens", 0),
                output_tokens=read("output_tokens", 0),
                metadata=metadata,
            )
        except Exception:
            self.run.client._count("dropped")
