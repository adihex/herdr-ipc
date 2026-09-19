#!/usr/bin/env python3
"""Push-based Herdr orchestrator listener.

Binds exclusively to:

    ${HERDR_IPC_SOCKET_DIR:-/tmp}/herdr_${HERDR_MACHINE_ID}_${HERDR_WORKSPACE_ID}.sock

Wakeups come from the kernel (kqueue/epoll via asyncio). There is no poll
loop on the socket. Each accepted envelope is identity-checked against the
bound (machine, workspace) pair and HMAC-verified with a 256-bit key written
next to the socket at mode 0600.
"""

from __future__ import annotations

import argparse
import asyncio
import errno
import json
import os
import signal
import socket
import struct
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque

from protocol import MAX_LINE_BYTES, ProtocolError, canonical_bytes, parse_line
from routing import (
    RoutingError,
    generate_key,
    key_path,
    load_identity,
    production_socket_path,
    read_key,
    socket_path,
    validate_id,
    verify,
    write_key,
)

BACKLOG = 256
MAX_INBOX = 50_000
SO_PEERCRED = 17
_PEER_RESET = (ConnectionResetError, BrokenPipeError)


def _quiet_peer_reset(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
    exc = context.get("exception")
    if isinstance(exc, _PEER_RESET):
        return
    loop.default_exception_handler(context)


class IsolationError(ProtocolError):
    """Envelope targeted the wrong orchestrator."""


OnMessage = Callable[[dict[str, Any]], None]
OnReject = Callable[[dict[str, Any], str], None]


def _peer_uid(sock: socket.socket | None) -> int | None:
    """Best-effort peer UID. Linux SO_PEERCRED only; macOS is 0600+HMAC."""
    if sock is None or not sys.platform.startswith("linux"):
        return None
    try:
        raw = sock.getsockopt(socket.SOL_SOCKET, SO_PEERCRED, struct.calcsize("3i"))
        _pid, uid, _gid = struct.unpack("3i", raw)
        return int(uid)
    except OSError:
        return None


def _reclaim_stale(path: Path) -> None:
    if not path.exists():
        return
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.05)
        probe.connect(str(path))
    except OSError as exc:
        stale = exc.errno in (
            errno.ECONNREFUSED,
            errno.ENOENT,
            errno.ECONNRESET,
            errno.ENOTCONN,
        ) or isinstance(exc, FileNotFoundError)
        if stale:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            return
        raise
    finally:
        probe.close()
    raise OSError(errno.EADDRINUSE, f"live orchestrator already bound at {path}")


class OrchestratorDaemon:
    """Scoped AF_UNIX SOCK_STREAM listener for one (machine, workspace)."""

    def __init__(
        self,
        machine_id: str,
        workspace_id: str,
        socket_dir: str | os.PathLike[str] | None = None,
        on_message: OnMessage | None = None,
        on_reject: OnReject | None = None,
        *,
        ack: bool = True,
        key: bytes | None = None,
    ) -> None:
        self.machine_id = validate_id(machine_id, "HERDR_MACHINE_ID")
        self.workspace_id = validate_id(workspace_id, "HERDR_WORKSPACE_ID")
        self.socket_path = socket_path(self.machine_id, self.workspace_id, socket_dir)
        self.key_path = key_path(self.socket_path)
        self.production_path = production_socket_path(self.machine_id, self.workspace_id)
        self.on_message = on_message
        self.on_reject = on_reject
        self.ack = ack
        self._provided_key = key
        self.key: bytes | None = None
        self.accepted: Deque[dict[str, Any]] = deque(maxlen=MAX_INBOX)
        self.rejected: Deque[dict[str, Any]] = deque(maxlen=MAX_INBOX)
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._aio_stop: asyncio.Event | None = None
        self._server: asyncio.AbstractServer | None = None
        self._start_error: BaseException | None = None
        self.started_at_ns: int | None = None

    @property
    def ready(self) -> bool:
        return self._ready.is_set() and self._start_error is None

    def start(self, timeout: float = 5.0) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._ready.clear()
        self._shutdown.clear()
        self._start_error = None
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"herdr-orch-{self.machine_id}-{self.workspace_id}",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=timeout):
            raise RuntimeError(f"orchestrator failed to bind {self.socket_path}")
        if self._start_error is not None:
            raise RuntimeError(f"orchestrator bind failed: {self._start_error}") from self._start_error

    def stop(self, timeout: float = 5.0) -> None:
        self._shutdown.set()
        loop = self._loop
        stop = self._aio_stop
        if loop is not None and stop is not None and loop.is_running():
            loop.call_soon_threadsafe(stop.set)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._cleanup_files()

    def wait_ready(self, timeout: float = 5.0) -> None:
        if not self._ready.wait(timeout=timeout):
            raise TimeoutError("orchestrator not listening")
        if self._start_error is not None:
            raise RuntimeError(str(self._start_error))

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.accepted)

    def reject_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.rejected)

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._amain())
        except Exception as exc:  # noqa: BLE001 — surface bind failures to start()
            self._start_error = exc
            self._ready.set()

    async def _amain(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop.set_exception_handler(_quiet_peer_reset)
        self._aio_stop = asyncio.Event()
        await self._bind()
        self.started_at_ns = time.time_ns()
        self._ready.set()
        try:
            await self._aio_stop.wait()
        finally:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
            self._cleanup_files()

    async def _bind(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        _reclaim_stale(self.socket_path)
        self._server = await asyncio.start_unix_server(
            self._handle,
            path=str(self.socket_path),
            backlog=BACKLOG,
        )
        os.chmod(self.socket_path, 0o600)
        self.key = self._provided_key if self._provided_key is not None else generate_key()
        write_key(self.key_path, self.key)

    def _cleanup_files(self) -> None:
        for path in (self.socket_path, self.key_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Fire-and-forget clients close after send. Accepting the envelope is
        # the contract; writing an ACK must never fail the handler or stall
        # the event loop with ConnectionResetError tracebacks.
        try:
            sock = writer.get_extra_info("socket")
            uid = _peer_uid(sock)
            if uid is not None and uid != os.getuid():
                return
            while True:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
                    break
                if not line:
                    break
                t_recv_ns = time.time_ns()
                if len(line) > MAX_LINE_BYTES:
                    self._reject({}, "too_large", t_recv_ns)
                    await self._try_ack(writer, {"ok": False, "error": "too_large"})
                    break
                try:
                    accepted = self._accept_line(line, t_recv_ns)
                    await self._try_ack(
                        writer,
                        {"ok": True, "t_recv_ns": accepted["t_recv_ns"], "latency_ns": accepted["latency_ns"]},
                    )
                except (ProtocolError, IsolationError, RoutingError) as exc:
                    await self._try_ack(writer, {"ok": False, "error": str(exc)})
        except (ConnectionResetError, BrokenPipeError, OSError, asyncio.CancelledError):
            return
        finally:
            await self._close_writer(writer)

    async def _try_ack(self, writer: asyncio.StreamWriter, body: dict[str, Any]) -> None:
        if not self.ack:
            return
        try:
            writer.write(json.dumps(body, separators=(",", ":")).encode("utf-8") + b"\n")
            await asyncio.wait_for(writer.drain(), timeout=0.005)
        except (ConnectionResetError, BrokenPipeError, OSError, asyncio.TimeoutError):
            return

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            return

    def _accept_line(self, line: bytes, t_recv_ns: int) -> dict[str, Any]:
        try:
            envelope = parse_line(line)
        except ProtocolError as exc:
            self._reject({"raw_bytes": len(line)}, str(exc), t_recv_ns)
            raise
        try:
            self._enforce_isolation(envelope)
        except IsolationError as exc:
            self._reject(envelope, str(exc), t_recv_ns)
            raise
        latency = max(0, t_recv_ns - int(envelope["t_send_ns"]))
        record = dict(envelope)
        record["t_recv_ns"] = t_recv_ns
        record["latency_ns"] = latency
        record["orchestrator_machine_id"] = self.machine_id
        record["orchestrator_workspace_id"] = self.workspace_id
        with self._lock:
            self.accepted.append(record)
        if self.on_message is not None:
            self.on_message(record)
        return record

    def _enforce_isolation(self, envelope: dict[str, Any]) -> None:
        if envelope["machine_id"] != self.machine_id or envelope["workspace_id"] != self.workspace_id:
            raise IsolationError(
                "identity mismatch: envelope "
                f"{envelope['machine_id']}/{envelope['workspace_id']} "
                f"!= socket {self.machine_id}/{self.workspace_id}"
            )
        if self.key is None:
            raise IsolationError("orchestrator HMAC key is not installed")
        digest = envelope.get("hmac", "")
        if not verify(self.key, canonical_bytes(envelope), digest):
            raise IsolationError("HMAC verification failed")

    def _reject(self, envelope: dict[str, Any], reason: str, t_recv_ns: int) -> None:
        record = {
            "reason": reason,
            "t_recv_ns": t_recv_ns,
            "machine_id": envelope.get("machine_id"),
            "workspace_id": envelope.get("workspace_id"),
            "pane_id": envelope.get("pane_id"),
            "nonce": envelope.get("nonce"),
        }
        with self._lock:
            self.rejected.append(record)
        if self.on_reject is not None:
            self.on_reject(record, reason)


def process_main(
    machine_id: str,
    workspace_id: str,
    socket_dir: str,
    inbox: Any,
    rejects: Any,
    ready: Any,
    stop: Any,
) -> None:
    """Child-process entry: one orchestrator, independent of the worker GIL."""

    def on_reject(record: dict[str, Any], _reason: str) -> None:
        rejects.put(record)

    daemon = OrchestratorDaemon(
        machine_id,
        workspace_id,
        socket_dir=socket_dir,
        on_message=inbox.put,
        on_reject=on_reject,
        ack=True,
    )
    try:
        daemon.start()
        ready.set()
        stop.wait()
    finally:
        daemon.stop()


def _install_signal_stop(daemon: OrchestratorDaemon) -> None:
    def _stop(signum: int, _frame: Any) -> None:
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Herdr orchestrator IPC daemon")
    parser.add_argument("--socket-dir", default=None, help="override HERDR_IPC_SOCKET_DIR (default /tmp)")
    parser.add_argument("--print-socket", action="store_true", help="print the bound path and exit after listen")
    parser.add_argument("--no-ack", action="store_true", help="do not write ACKs (pure fire-and-forget)")
    parser.add_argument("--jsonl", default=None, help="append accepted envelopes to this JSONL file")
    args = parser.parse_args(argv)

    try:
        machine, workspace, _pane = load_identity()
    except RoutingError as exc:
        print(f"herdr-orchestrator: {exc}", file=sys.stderr)
        return 2

    jsonl_path = Path(args.jsonl).expanduser() if args.jsonl else None
    jsonl_lock = threading.Lock()

    def persist(record: dict[str, Any]) -> None:
        if jsonl_path is None:
            return
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        with jsonl_lock:
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with jsonl_path.open("a", encoding="utf-8") as handle:
                handle.write(line)

    daemon = OrchestratorDaemon(
        machine,
        workspace,
        socket_dir=args.socket_dir,
        on_message=persist,
        ack=not args.no_ack,
    )
    try:
        daemon.start()
    except (OSError, RuntimeError, RoutingError) as exc:
        print(f"herdr-orchestrator: bind failed: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "event": "listening",
                "socket": str(daemon.socket_path),
                "production_socket": str(daemon.production_path),
                "machine_id": daemon.machine_id,
                "workspace_id": daemon.workspace_id,
                "key_path": str(daemon.key_path),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    if args.print_socket:
        daemon.stop()
        return 0

    _install_signal_stop(daemon)
    thread = daemon._thread
    if thread is None:
        return 1
    thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
