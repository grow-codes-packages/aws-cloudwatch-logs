"""Live Tail sessions (``StartLiveTail`` event streams) for the sync and async clients."""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
from collections.abc import Callable, Iterator, Mapping
from typing import Any

from ._aws_errors import map_aws_error
from ._core import CloudWatchLogsError, Secret
from ._logging import WORKER_THREAD_PREFIX
from ._ops import _dt, _dt0
from ._types import LiveTailEvent, LiveTailUpdate

__all__ = ["AsyncLiveTailSession", "LiveTailSession"]

_OP = "start_live_tail"

OnEvent = Callable[[LiveTailEvent], None]
OnUpdate = Callable[[LiveTailUpdate], None]
OnError = Callable[[CloudWatchLogsError], None]


def _safe(callback: Callable[[Any], None] | None, value: Any) -> None:
    if callback is None:
        return
    with contextlib.suppress(Exception):  # callbacks must never break the session
        callback(value)


class _Dispatcher:
    """Turns event-stream frames into callbacks (shared by both session kinds)."""

    def __init__(
        self,
        on_event: OnEvent,
        on_update: OnUpdate | None,
        on_error: OnError | None,
        known_secrets: list[str | Secret | None],
    ) -> None:
        self.on_event = on_event
        self.on_update = on_update
        self.on_error = on_error
        self.known_secrets = known_secrets

    def deliver(self, frame: Mapping[str, Any]) -> None:
        update = frame.get("sessionUpdate")
        if not isinstance(update, Mapping):
            return
        results = update.get("sessionResults") or []
        sampled = bool((update.get("sessionMetadata") or {}).get("sampled"))
        _safe(self.on_update, LiveTailUpdate(sampled=sampled, count=len(results)))
        for r in results:
            _safe(
                self.on_event,
                LiveTailEvent(
                    timestamp=_dt0(r.get("timestamp")),
                    message=str(r.get("message") or ""),
                    ingestion_time=_dt(r.get("ingestionTime")),
                    log_stream_name=r.get("logStreamName"),
                    log_group_identifier=r.get("logGroupIdentifier"),
                ),
            )

    def map(self, exc: BaseException) -> CloudWatchLogsError:
        return map_aws_error(_OP, exc, self.known_secrets)


class LiveTailSession:
    """A running Live Tail session of :class:`~grow_aws_cloudwatch_logs.CloudWatchLogsClient`.

    Frames are read by a daemon thread that calls ``on_event`` for every event (and
    ``on_update`` for every ``sessionUpdate`` frame, also empty keep-alive updates). The session
    ends when the service closes the stream (Moto: after replaying the matching existing events;
    AWS: after up to 3 hours), on an error (passed to ``on_error`` and kept in :attr:`error`) or
    on :meth:`close`. Exceptions raised by callbacks are ignored.
    """

    def __init__(
        self,
        stream: Any,
        dispatcher: _Dispatcher,
        on_done: Callable[[LiveTailSession], None] | None = None,
    ) -> None:
        self._stream = stream
        self._dispatcher = dispatcher
        self._on_done = on_done
        self._iterator: Iterator[Any] = iter(stream)
        self._stopping = False
        self._done = threading.Event()
        self._error: CloudWatchLogsError | None = None
        self._thread: threading.Thread | None = None
        #: Session id from the ``sessionStart`` frame (empty when the stream ended before it).
        self.session_id = ""

    def _start(self) -> None:
        """Read frames until ``sessionStart`` (in the caller's thread), then start the pump."""
        ended = False
        try:
            for frame in self._iterator:
                start = frame.get("sessionStart")
                if isinstance(start, Mapping):
                    self.session_id = str(start.get("sessionId") or "")
                    break
                self._dispatcher.deliver(frame)
            else:
                ended = True
        except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
            self._close_stream()
            mapped = self._dispatcher.map(exc)
            raise mapped from mapped.cause
        if ended:
            self._finish()
            return
        self._thread = threading.Thread(
            target=self._pump, name=f"{WORKER_THREAD_PREFIX}-tail", daemon=True
        )
        self._thread.start()

    def _pump(self) -> None:
        try:
            for frame in self._iterator:
                if self._stopping:
                    break
                self._dispatcher.deliver(frame)
        except Exception as exc:  # noqa: BLE001 - reported through on_error / error
            if not self._stopping:
                self._error = self._dispatcher.map(exc)
                _safe(self._dispatcher.on_error, self._error)
        finally:
            self._close_stream()
            self._finish()

    def _finish(self) -> None:
        if self._done.is_set():
            return
        self._done.set()
        if self._on_done is not None:
            with contextlib.suppress(Exception):
                self._on_done(self)

    def _close_stream(self) -> None:
        raw: Any = getattr(self._stream, "_raw_stream", None)
        # Interrupt a read blocked in the reader thread (urllib3 >= 2.3), else shut the socket.
        with contextlib.suppress(Exception):
            if hasattr(raw, "shutdown"):
                raw.shutdown()
            elif raw is not None:  # pragma: no cover - older urllib3
                raw.connection.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(Exception):
            self._stream.close()

    @property
    def closed(self) -> bool:
        """Whether the session has ended."""
        return self._done.is_set()

    @property
    def error(self) -> CloudWatchLogsError | None:
        """The error that ended the session, if any."""
        return self._error

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the session ended (or ``timeout`` seconds); returns whether it ended."""
        return self._done.wait(timeout)

    def close(self) -> None:
        """Stop the session and wait (up to 5 s) for the reader thread; idempotent."""
        self._stopping = True
        self._close_stream()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(5.0)
        self._finish()

    #: Alias of :meth:`close` (``TailHandle`` protocol).
    stop = close

    def __repr__(self) -> str:
        return f"LiveTailSession(session_id={self.session_id!r}, closed={self.closed})"


class AsyncLiveTailSession:
    """A running Live Tail session of :class:`~grow_aws_cloudwatch_logs.AsyncCloudWatchLogsClient`.

    Frames are read by an asyncio task on the client's event loop; behaviour as
    :class:`LiveTailSession`. :meth:`close` and :meth:`stop` are coroutines.
    """

    def __init__(
        self,
        stream: Any,
        dispatcher: _Dispatcher,
        on_done: Callable[[AsyncLiveTailSession], None] | None = None,
    ) -> None:
        self._stream = stream
        self._dispatcher = dispatcher
        self._on_done = on_done
        self._iterator = stream.__aiter__()
        self._stopping = False
        self._done = asyncio.Event()
        self._error: CloudWatchLogsError | None = None
        self._task: asyncio.Task[None] | None = None
        #: Session id from the ``sessionStart`` frame (empty when the stream ended before it).
        self.session_id = ""

    async def _start(self) -> None:
        ended = False
        try:
            while True:
                try:
                    frame = await self._iterator.__anext__()
                except StopAsyncIteration:
                    ended = True
                    break
                start = frame.get("sessionStart")
                if isinstance(start, Mapping):
                    self.session_id = str(start.get("sessionId") or "")
                    break
                self._dispatcher.deliver(frame)
        except Exception as exc:  # noqa: BLE001 - every SDK failure is mapped
            self._close_stream()
            mapped = self._dispatcher.map(exc)
            raise mapped from mapped.cause
        if ended:
            self._finish()
            return
        self._task = asyncio.get_running_loop().create_task(self._pump())

    async def _pump(self) -> None:
        try:
            while not self._stopping:
                try:
                    frame = await self._iterator.__anext__()
                except StopAsyncIteration:
                    break
                self._dispatcher.deliver(frame)
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001 - reported through on_error / error
            if not self._stopping:
                self._error = self._dispatcher.map(exc)
                _safe(self._dispatcher.on_error, self._error)
        finally:
            self._close_stream()
            self._finish()

    def _finish(self) -> None:
        if self._done.is_set():
            return
        self._done.set()
        if self._on_done is not None:
            with contextlib.suppress(Exception):
                self._on_done(self)

    def _close_stream(self) -> None:
        with contextlib.suppress(Exception):
            self._stream.close()

    @property
    def closed(self) -> bool:
        """Whether the session has ended."""
        return self._done.is_set()

    @property
    def error(self) -> CloudWatchLogsError | None:
        """The error that ended the session, if any."""
        return self._error

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait until the session ended (or ``timeout`` seconds); returns whether it ended."""
        try:
            await asyncio.wait_for(self._done.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True

    async def close(self) -> None:
        """Stop the session and wait for the reader task; idempotent."""
        self._stopping = True
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._close_stream()
        self._finish()

    #: Alias of :meth:`close` (``AsyncTailHandle`` protocol).
    stop = close

    def __repr__(self) -> str:
        return f"AsyncLiveTailSession(session_id={self.session_id!r}, closed={self.closed})"
