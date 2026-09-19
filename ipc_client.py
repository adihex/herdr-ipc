#!/usr/bin/env python3
"""Non-blocking Herdr IPC push client.

Drops one NDJSON envelope on the scoped Unix socket and returns. The worker
never waits for orchestrator processing. Connect+send is bounded by a short
timeout so a missing daemon fails fast instead of hanging a pane.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping

from protocol import (
    ProtocolError,
    build_envelope,
    canonical_bytes,
    encode_line,
    normalize_status,
)
from routing import (
    RoutingError,
    key_path,
    load_identity,
    production_socket_path,
    read_key,
    sign,
    socket_path,
)

SO_NOSIGPIPE = getattr(socket, "SO_NOSIGPIPE", 0x1022)
DEFAULT_TIMEOUT_S = 0.1  # 100ms cap — drop, don't stall the worker


class PushError(RuntimeError):
    """The payload could not be dropped on the scoped socket."""


@dataclass(frozen=True)
class PushResult:
    ok: bool
    socket: str
    bytes_sent: int
    send_ns: int
    nonce: str | None = None
    t_send_ns: int | None = None
    ack: dict[str, Any] | None = None
    error: str | None = None


def _set_nosigpipe(sock: socket.socket) -> None:
    try:
        sock.setsockopt(socket.SOL_SOCKET, SO_NOSIGPIPE, 1)
    except (OSError, AttributeError):
        pass


def _connect_and_send(path: str, payload: bytes, timeout_s: float) -> int:
    """Drop bytes on the Unix socket and return.

    Bounded by `timeout_s` so a missing daemon cannot hang the worker. Does
    not read an ACK: orchestrator processing is never on the worker's critical
    path. Local AF_UNIX connect+sendall is typically well under a millisecond.
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    _set_nosigpipe(sock)
    sock.settimeout(timeout_s)
    try:
        sock.connect(path)
        sock.sendall(payload)
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        return len(payload)
    except socket.timeout as exc:
        raise TimeoutError("ipc push timed out") from exc
    finally:
        sock.close()


def _recv_ack(path: str, payload: bytes, timeout_s: float) -> tuple[int, dict[str, Any]]:
    """Test/debug path: send and read one ACK line. Not used by the hook."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    _set_nosigpipe(sock)
    sock.settimeout(timeout_s)
    try:
        sock.connect(path)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while True:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
            if b"\n" in data:
                break
        raw = b"".join(chunks).split(b"\n", 1)[0]
        ack = json.loads(raw.decode("utf-8") or "{}")
        if not isinstance(ack, dict):
            raise PushError("ACK is not a JSON object")
        return len(payload), ack
    finally:
        sock.close()


def push(
    status: str,
    payload: Mapping[str, Any] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    socket_dir: str | os.PathLike[str] | None = None,
    wait_ack: bool = False,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    t_send_ns: int | None = None,
    extra: Mapping[str, Any] | None = None,
) -> PushResult:
    env = dict(os.environ if environ is None else environ)
    machine, workspace, pane = load_identity(env)
    path = socket_path(machine, workspace, socket_dir if socket_dir is not None else env.get("HERDR_IPC_SOCKET_DIR"))
    key = read_key(key_path(path))
    body: dict[str, Any] = dict(payload or {})
    if extra:
        body.update(extra)
    send_ns = t_send_ns if t_send_ns is not None else time.time_ns()
    envelope = build_envelope(
        machine_id=machine,
        workspace_id=workspace,
        pane_id=pane,
        status=status,
        payload=body,
        t_send_ns=send_ns,
    )
    envelope["hmac"] = sign(key, canonical_bytes(envelope))
    line = encode_line(envelope)
    started = time.perf_counter_ns()
    try:
        if wait_ack:
            sent, ack = _recv_ack(str(path), line, timeout_s)
            return PushResult(
                ok=bool(ack.get("ok")),
                socket=str(path),
                bytes_sent=sent,
                send_ns=time.perf_counter_ns() - started,
                nonce=envelope["nonce"],
                t_send_ns=envelope["t_send_ns"],
                ack=ack,
            )
        sent = _connect_and_send(str(path), line, timeout_s)
        return PushResult(
            ok=True,
            socket=str(path),
            bytes_sent=sent,
            send_ns=time.perf_counter_ns() - started,
            nonce=envelope["nonce"],
            t_send_ns=envelope["t_send_ns"],
        )
    except FileNotFoundError as exc:
        raise PushError(f"orchestrator socket missing: {path}") from exc
    except TimeoutError as exc:
        raise PushError(f"push timed out after {timeout_s * 1000:.0f}ms: {path}") from exc
    except OSError as exc:
        raise PushError(f"push failed ({exc.strerror}): {path}") from exc


def push_forged_for_tests(
    *,
    target_machine: str,
    target_workspace: str,
    envelope: dict[str, Any],
    socket_dir: str | os.PathLike[str] | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    wait_ack: bool = True,
    hmac_key: bytes | None = None,
) -> dict[str, Any]:
    """Adversarial helper used only by the isolation suite."""
    path = socket_path(target_machine, target_workspace, socket_dir)
    msg = dict(envelope)
    if hmac_key is not None:
        msg["hmac"] = sign(hmac_key, canonical_bytes(msg))
    elif "hmac" not in msg:
        msg["hmac"] = "0" * 64
    line = encode_line(msg)
    try:
        _sent, ack = _recv_ack(str(path), line, timeout_s)
        return ack
    except Exception as exc:  # noqa: BLE001 — attack outcome is the result
        return {"ok": False, "error": str(exc)}


def _merge_payload(args_payload: str | None, stdin_json: str | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for raw in (args_payload, stdin_json):
        if not raw or not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PushError(f"payload is not JSON: {exc.msg}") from exc
        if not isinstance(value, dict):
            raise PushError("payload must be a JSON object")
        merged.update(value)
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Drop a status envelope on the scoped Herdr socket")
    parser.add_argument("status", nargs="?", default=None, help="working | blocked | done")
    parser.add_argument("payload", nargs="?", default=None, help="JSON object string")
    parser.add_argument("--status", dest="status_flag", default=None)
    parser.add_argument("--payload", dest="payload_flag", default=None)
    parser.add_argument("--socket-dir", default=None)
    parser.add_argument("--wait-ack", action="store_true")
    parser.add_argument("--timeout-ms", type=float, default=DEFAULT_TIMEOUT_S * 1000)
    parser.add_argument("--print-socket", action="store_true")
    args = parser.parse_args(argv)

    try:
        machine, workspace, pane = load_identity()
    except RoutingError as exc:
        print(f"herdr-push: {exc}", file=sys.stderr)
        return 2

    path = socket_path(machine, workspace, args.socket_dir)
    if args.print_socket:
        print(
            json.dumps(
                {
                    "socket": str(path),
                    "production_socket": str(production_socket_path(machine, workspace)),
                    "machine_id": machine,
                    "workspace_id": workspace,
                    "pane_id": pane,
                },
                separators=(",", ":"),
            )
        )
        return 0

    status = args.status_flag or args.status or os.environ.get("HERDR_STATUS") or "working"
    stdin_json = None
    if not sys.stdin.isatty():
        stdin_json = sys.stdin.read()

    try:
        payload = _merge_payload(args.payload_flag or args.payload, stdin_json)
        normalize_status(status)
        result = push(
            status,
            payload,
            socket_dir=args.socket_dir,
            wait_ack=args.wait_ack,
            timeout_s=max(args.timeout_ms, 1.0) / 1000.0,
        )
    except (PushError, RoutingError, ProtocolError) as exc:
        print(f"herdr-push: {exc}", file=sys.stderr)
        return 1

    if args.wait_ack:
        print(json.dumps({"ok": result.ok, "ack": result.ack, "send_ns": result.send_ns}, separators=(",", ":")))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
