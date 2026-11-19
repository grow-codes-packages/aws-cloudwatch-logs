"""Test support: programmable local HTTP servers (generic + AWS JSON 1.1), event-stream
encoding, fixtures and config helpers."""

from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import dataclasses
import json
import socket
import struct
import threading
import time
import zlib
from collections import defaultdict, deque
from collections.abc import Awaitable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import grow_aws_cloudwatch_logs as sdk


@dataclasses.dataclass
class Reply:
    status: int = 200
    body: Any = None
    headers: dict[str, str] = dataclasses.field(default_factory=dict)
    delay: float = 0.0  # before the status line
    drip: float = 0.0  # pause between body chunks (slow body)


@dataclasses.dataclass
class Seen:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class MockServer:
    """Threaded HTTP server; queue replies per path, inspect the requests it saw."""

    def __init__(self) -> None:
        self.replies: dict[str, deque[Reply]] = defaultdict(deque)
        self.defaults: dict[str, Reply] = {}
        self.seen: list[Seen] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def _handle(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                body = self.rfile.read(length) if length else b""
                path = self.path.split("?", 1)[0]
                server.seen.append(Seen(self.command, self.path, dict(self.headers), body))
                queue = server.replies.get(path)
                reply = queue.popleft() if queue else server.defaults.get(path, Reply())
                if reply.delay:
                    time.sleep(reply.delay)
                if isinstance(reply.body, (bytes, str)):
                    payload = reply.body.encode() if isinstance(reply.body, str) else reply.body
                    ctype = "text/plain"
                else:
                    payload = json.dumps(reply.body if reply.body is not None else {}).encode()
                    ctype = "application/json"
                try:
                    self.send_response(reply.status)
                    self.send_header("Content-Type", ctype)
                    for k, v in reply.headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    if reply.drip:
                        for i in range(len(payload)):
                            self.wfile.write(payload[i : i + 1])
                            self.wfile.flush()
                            time.sleep(reply.drip)
                    else:
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _handle

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host!s}:{port}"

    def enqueue(self, path: str, *replies: Reply) -> None:
        self.replies[path].extend(replies)

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def no_jitter() -> float:
    return 0.0


def fast_retry(**kw: Any) -> sdk.RetryPolicy:
    values: dict[str, Any] = {"initial_backoff": 0.01, "max_backoff": 0.05, "random": no_jitter}
    values.update(kw)
    return sdk.RetryPolicy(**values)


@dataclasses.dataclass(frozen=True, repr=False)
class TokenConfig(sdk.CloudWatchLogsConfig):
    """Config with a bearer ``token`` field, used by the generated-core transport tests."""

    token: sdk.Secret | None = None

    @classmethod
    def _env_values(cls, reader: sdk.EnvReader) -> dict[str, Any]:
        values = super()._env_values(reader)
        token = reader.string("TOKEN", cls.ENV_VENDOR.get("TOKEN", ()))
        if token is not None:
            values["token"] = sdk.Secret(token)
        return values


def make_config(url: str, **kw: Any) -> TokenConfig:
    """A config for the core transport tests (``token`` supported)."""
    kw.setdefault("retry", fast_retry())
    return TokenConfig(url=url, **kw)


_REFUSING: list[socket.socket] = []
atexit.register(lambda: [sock.close() for sock in _REFUSING])


def closed_port_url() -> str:
    """URL of a port that refuses connections.

    The socket stays bound (never listening) for the whole test session: connects are refused,
    and no other server (a parallel MockServer, another process) can take the port meanwhile,
    which a bind-then-close port would allow.
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    _REFUSING.append(sock)
    port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def spawn_capturing(awaitable: Awaitable[Any], sink: list[BaseException]) -> asyncio.Task[Any]:
    """Run ``awaitable`` as a task and record the exception it raises *inside* the task.

    On Python < 3.11 awaiting a cancelled task raises a fresh ``asyncio.CancelledError``, so the
    SDK's ``CancelledError`` subclass is only observable from within the task itself.
    """

    async def run() -> Any:
        try:
            return await awaitable
        except BaseException as exc:
            sink.append(exc)
            raise

    return asyncio.ensure_future(run())


# --------------------------------------------------------------------------- AWS mock

FIXTURES = Path(__file__).parent / "fixtures"
TEST_KEY = "AKIATESTKEY000000000"
TEST_SECRET = "test-secret-access-key-0123456789"


def fixture(name: str) -> dict[str, Any]:
    """A recorded fixture (``tests/fixtures/<name>.json``)."""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _crc(data: bytes) -> bytes:
    return struct.pack(">I", zlib.crc32(data) & 0xFFFFFFFF)


def event_frame(headers: dict[str, str], payload: bytes | dict[str, Any] | None) -> bytes:
    """One AWS event-stream message (string headers only)."""
    body = (
        b""
        if payload is None
        else payload
        if isinstance(payload, bytes)
        else json.dumps(payload).encode()
    )
    raw_headers = b""
    for name, value in headers.items():
        n, v = name.encode(), value.encode()
        raw_headers += struct.pack(">B", len(n)) + n + b"\x07" + struct.pack(">H", len(v)) + v
    total = 12 + len(raw_headers) + len(body) + 4
    prelude = struct.pack(">II", total, len(raw_headers))
    message = prelude + _crc(prelude) + raw_headers + body
    return message + _crc(message)


def event(kind: str, payload: dict[str, Any] | None) -> bytes:
    """An ``event`` frame of type ``kind`` with a JSON payload."""
    headers = {":message-type": "event", ":event-type": kind}
    if payload is not None:
        headers[":content-type"] = "application/json"
    return event_frame(headers, payload)


def exception_frame(kind: str, message: str = "boom") -> bytes:
    """An ``exception`` frame (e.g. ``SessionStreamingException``)."""
    return event_frame(
        {
            ":message-type": "exception",
            ":exception-type": kind,
            ":content-type": "application/json",
        },
        {"message": message},
    )


def session_start(session_id: str = "sess-1") -> bytes:
    return event("sessionStart", {"requestId": "r-1", "sessionId": session_id})


def session_update(*results: dict[str, Any], sampled: bool = False) -> bytes:
    return event(
        "sessionUpdate",
        {"sessionMetadata": {"sampled": sampled}, "sessionResults": list(results)},
    )


@dataclasses.dataclass
class AwsReply:
    status: int = 200
    body: Any = None  # dict → JSON; str/bytes → raw; None → empty
    headers: dict[str, str] = dataclasses.field(default_factory=dict)
    delay: float = 0.0
    stream: bool = False  # application/vnd.amazon.eventstream, no Content-Length
    hold: threading.Event | None = None  # keep a stream open until set (max 10 s)
    drop: bool = False  # close the connection without answering


@dataclasses.dataclass
class AwsSeen:
    op: str
    host: str
    headers: dict[str, str]
    body: Any


class AwsMock:
    """Threaded local AWS JSON 1.1 endpoint: replies are queued per operation
    (``X-Amz-Target: Logs_20140328.<Op>``); requests are recorded with parsed bodies."""

    def __init__(self) -> None:
        self.replies: dict[str, deque[AwsReply]] = defaultdict(deque)
        self.seen: list[AwsSeen] = []
        mock = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length) if length else b""
                target = self.headers.get("x-amz-target") or ""
                op = target.rsplit(".", 1)[-1]
                try:
                    body = json.loads(raw) if raw else None
                except ValueError:
                    body = raw
                mock.seen.append(
                    AwsSeen(op, self.headers.get("host", ""), dict(self.headers), body)
                )
                queue = mock.replies.get(op)
                reply = (
                    queue.popleft()
                    if queue
                    else AwsReply(500, f"no reply queued for {op}", {"content-type": "text/html"})
                )
                if reply.delay:
                    time.sleep(reply.delay)
                if reply.drop:
                    self.close_connection = True
                    with contextlib.suppress(OSError):
                        self.connection.shutdown(socket.SHUT_RDWR)
                    return
                if isinstance(reply.body, bytes):
                    payload = reply.body
                elif isinstance(reply.body, str):
                    payload = reply.body.encode()
                elif reply.body is None:
                    payload = b""
                else:
                    payload = json.dumps(reply.body).encode()
                try:
                    self.send_response(reply.status)
                    headers = {
                        "x-amzn-requestid": "req-0001",
                        "content-type": "application/vnd.amazon.eventstream"
                        if reply.stream
                        else "application/x-amz-json-1.1",
                    }
                    headers.update({k.lower(): v for k, v in reply.headers.items()})
                    headers.pop("content-length", None)
                    headers.pop("status", None)
                    for k, v in headers.items():
                        self.send_header(k, v)
                    if reply.stream:
                        # chunked, like AWS: the SDK sees each chunk as soon as it arrives
                        self.send_header("Transfer-Encoding", "chunked")
                        self.send_header("Connection", "close")
                        self.close_connection = True
                        self.end_headers()
                        if payload:
                            self.wfile.write(b"%x\r\n%s\r\n" % (len(payload), payload))
                            self.wfile.flush()
                        if reply.hold is not None:
                            reply.hold.wait(10)
                        self.wfile.write(b"0\r\n\r\n")
                        return
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        )
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host!s}:{port}"

    def reply(self, op: str, body: Any = None, status: int = 200, **kw: Any) -> AwsMock:
        """Queue one reply for ``op``."""
        self.replies[op].append(AwsReply(status=status, body=body, **kw))
        return self

    def error(self, op: str, code: str, status: int = 400, message: str = "boom") -> AwsMock:
        """Queue an AWS JSON error reply."""
        return self.reply(
            op, {"__type": code, "message": message}, status, headers={"x-amzn-ErrorType": code}
        )

    def fixture(self, name: str, op: str | None = None) -> AwsMock:
        """Queue the recorded response of ``fixture(name)`` for its operation."""
        data = fixture(name)
        response = data["response"]
        if "bodyBase64" in response:
            body: Any = base64.b64decode(response["bodyBase64"])
            return self.reply(op or data["operation"], body, response["status"], stream=True)
        return self.reply(
            op or data["operation"],
            response.get("body"),
            response["status"],
            headers=dict(response.get("headers") or {}),
        )

    def bodies(self, op: str) -> list[Any]:
        """Parsed request bodies sent for ``op``."""
        return [s.body for s in self.seen if s.op == op]

    def ops(self) -> list[str]:
        return [s.op for s in self.seen]

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def aws_config(url: str, **kw: Any) -> sdk.CloudWatchLogsConfig:
    """A CloudWatch Logs config for a local mock (static keys, us-east-1, no retries)."""
    kw.setdefault("retry", sdk.RetryPolicy(max_retries=0))
    kw.setdefault("region", "us-east-1")
    kw.setdefault("access_key_id", TEST_KEY)
    kw.setdefault("secret_access_key", TEST_SECRET)
    kw.setdefault("log_group", "app/main")
    kw.setdefault("log_stream", "s1")
    return sdk.CloudWatchLogsConfig(url=url, **kw)


INTERNALS = sdk.ClientInternals(inject_host_prefix=False, sleep=lambda s: None)
