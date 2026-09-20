"""NDJSON envelope for Herdr worker -> orchestrator pushes."""

from __future__ import annotations

import json
import secrets
import time
from typing import Any, Mapping

STATUSES = frozenset({"working", "blocked", "done", "idle", "unknown"})
STATUS_ALIASES = {
    "fail": "blocked",
    "failed": "blocked",
    "failure": "blocked",
    "error": "blocked",
    "running": "working",
    "complete": "done",
    "completed": "done",
}
PROTOCOL_VERSION = 2
MAX_LINE_BYTES = 64 * 1024
HMAC_FIELDS = (
    "v",
    "ts",
    "t_send_ns",
    "machine_id",
    "workspace_id",
    "pane_id",
    "session_id",
    "socket_id",
    "sender",
    "status",
    "nonce",
    "payload",
)
SENDER_KINDS = frozenset({"user", "agent", "supervisor", "system"})


class ProtocolError(ValueError):
    """Envelope is not a valid Herdr IPC message."""


def normalize_status(status: str | None) -> str:
    if not status:
        raise ProtocolError("status is required")
    key = status.strip().lower()
    key = STATUS_ALIASES.get(key, key)
    if key not in STATUSES:
        raise ProtocolError(f"status must be one of {sorted(STATUSES)} (got {status!r})")
    return key


def _payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ProtocolError("payload must be a JSON object")
    return value


def _sender(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError("sender must be a JSON object")
    required = {"kind": str, "id": str, "session_id": str, "pane_id": str}
    out: dict[str, Any] = {}
    for key, typ in required.items():
        item = value.get(key)
        if not isinstance(item, typ) or not item:
            raise ProtocolError(f"sender.{key} must be a non-empty string")
        out[key] = item
    if out["kind"] not in SENDER_KINDS:
        raise ProtocolError(f"sender.kind must be one of {sorted(SENDER_KINDS)}")
    name = value.get("name")
    if name is not None:
        if not isinstance(name, str) or not name:
            raise ProtocolError("sender.name must be a non-empty string when present")
        out["name"] = name
    return out


def canonical_bytes(envelope: Mapping[str, Any]) -> bytes:
    body = {field: envelope[field] for field in HMAC_FIELDS}
    return json.dumps(body, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")


def build_envelope(
    *,
    machine_id: str,
    workspace_id: str,
    pane_id: str,
    session_id: str,
    sender: Mapping[str, Any],
    status: str,
    payload: Mapping[str, Any] | None = None,
    t_send_ns: int | None = None,
    nonce: str | None = None,
) -> dict[str, Any]:
    now = time.time_ns()
    envelope: dict[str, Any] = {
        "v": PROTOCOL_VERSION,
        "ts": now,
        "t_send_ns": int(t_send_ns if t_send_ns is not None else now),
        "machine_id": machine_id,
        "workspace_id": workspace_id,
        "pane_id": pane_id,
        "session_id": session_id,
        "socket_id": f"{machine_id}/{workspace_id}",
        "sender": dict(sender),
        "status": normalize_status(status),
        "nonce": nonce or secrets.token_hex(16),
        "payload": dict(payload or {}),
    }
    return envelope


def encode_line(envelope: Mapping[str, Any]) -> bytes:
    line = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
    if len(line) > MAX_LINE_BYTES:
        raise ProtocolError(f"envelope exceeds {MAX_LINE_BYTES} bytes")
    return line


def parse_line(raw: bytes | str) -> dict[str, Any]:
    if isinstance(raw, bytes):
        if len(raw) > MAX_LINE_BYTES:
            raise ProtocolError("line too large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("envelope is not UTF-8") from exc
    else:
        text = raw
        if len(text.encode("utf-8")) > MAX_LINE_BYTES:
            raise ProtocolError("line too large")
    text = text.strip()
    if not text:
        raise ProtocolError("empty line")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"invalid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise ProtocolError("envelope must be a JSON object")
    return validate_envelope(value)


def validate_envelope(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("v") != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported protocol version {value.get('v')!r}")
    required = {
        "ts": int,
        "t_send_ns": int,
        "machine_id": str,
        "workspace_id": str,
        "pane_id": str,
        "session_id": str,
        "socket_id": str,
        "status": str,
        "nonce": str,
        "hmac": str,
    }
    out: dict[str, Any] = {"v": PROTOCOL_VERSION}
    for key, typ in required.items():
        if key not in value:
            raise ProtocolError(f"missing field {key}")
        item = value[key]
        if typ is int and isinstance(item, bool):
            raise ProtocolError(f"{key} must be an integer")
        if not isinstance(item, typ):
            raise ProtocolError(f"{key} must be {typ.__name__}")
        out[key] = item
    out["status"] = normalize_status(out["status"])
    out["sender"] = _sender(value.get("sender"))
    if out["sender"]["session_id"] != out["session_id"]:
        raise ProtocolError("sender.session_id must match session_id")
    if out["sender"]["pane_id"] != out["pane_id"]:
        raise ProtocolError("sender.pane_id must match pane_id")
    if out["socket_id"] != f"{out['machine_id']}/{out['workspace_id']}":
        raise ProtocolError("socket_id must match machine_id/workspace_id")
    out["payload"] = _payload(value.get("payload"))
    if len(out["nonce"]) > 128 or not out["nonce"]:
        raise ProtocolError("nonce is invalid")
    if len(out["hmac"]) != 64:
        raise ProtocolError("hmac must be a 64-char hex digest")
    return out
