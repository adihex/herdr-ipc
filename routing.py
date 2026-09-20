"""Cryptographic and structural routing isolation for Herdr IPC.

Socket names are a pure function of the invariant metadata Herdr injects into
every pane:

    /tmp/herdr_${HERDR_MACHINE_ID}_${HERDR_WORKSPACE_ID}.sock

A worker for Machine A / Project B therefore cannot address the orchestrator
for Machine A / Project C: the path is different, the HMAC key is different,
and the daemon drops any envelope whose identity does not match the socket
it bound. Cross-talk is a type error, not a runtime race.
"""

from __future__ import annotations

import hmac
import os
import re
import secrets
from pathlib import Path

# Herdr pane/tab IDs are workspace-qualified (`w2F:p2V`). Colons are allowed
# in identity fields; they never appear in socket filenames (machine+workspace).
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
DEFAULT_SOCKET_DIR = "/tmp"
DEFAULT_MACHINE_ID = "local"
SOCKET_PREFIX = "herdr_"
SOCKET_SUFFIX = ".sock"
KEY_SUFFIX = ".key"
# macOS sockaddr_un.sun_path is 104 bytes including NUL; keep paths ≤ 103.
MAX_SUN_PATH = 103
MAX_ID_LEN = 64


class RoutingError(ValueError):
    """Identity or socket-path construction failed; refuse to send or bind."""


def validate_id(value: str | None, name: str) -> str:
    if not value or not isinstance(value, str):
        raise RoutingError(f"{name} is required")
    if len(value) > MAX_ID_LEN:
        raise RoutingError(f"{name} exceeds {MAX_ID_LEN} characters")
    if not ID_RE.fullmatch(value):
        raise RoutingError(
            f"{name} must match {ID_RE.pattern} (got {value!r})"
        )
    return value


def socket_dir(explicit: str | os.PathLike[str] | None = None) -> Path:
    raw = explicit if explicit is not None else os.environ.get("HERDR_IPC_SOCKET_DIR", DEFAULT_SOCKET_DIR)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise RoutingError("HERDR_IPC_SOCKET_DIR must be an absolute path")
    return path


def socket_name(machine_id: str, workspace_id: str) -> str:
    machine = validate_id(machine_id, "HERDR_MACHINE_ID")
    workspace = validate_id(workspace_id, "HERDR_WORKSPACE_ID")
    return f"{SOCKET_PREFIX}{machine}_{workspace}{SOCKET_SUFFIX}"


def socket_path(
    machine_id: str,
    workspace_id: str,
    directory: str | os.PathLike[str] | None = None,
) -> Path:
    path = socket_dir(directory) / socket_name(machine_id, workspace_id)
    encoded = os.fsencode(path)
    if len(encoded) > MAX_SUN_PATH:
        raise RoutingError(
            f"AF_UNIX path exceeds {MAX_SUN_PATH} bytes ({len(encoded)}): {path}"
        )
    return path


def production_socket_path(machine_id: str, workspace_id: str) -> Path:
    """The contract path: always under /tmp, never overridden."""
    return Path(DEFAULT_SOCKET_DIR) / socket_name(machine_id, workspace_id)


def key_path(sock: Path) -> Path:
    return Path(str(sock) + KEY_SUFFIX)


def load_identity(environ: dict[str, str] | None = None) -> tuple[str, str, str]:
    env = environ if environ is not None else os.environ
    machine_raw = env.get("HERDR_MACHINE_ID") or DEFAULT_MACHINE_ID
    machine = validate_id(machine_raw, "HERDR_MACHINE_ID")
    workspace = validate_id(env.get("HERDR_WORKSPACE_ID"), "HERDR_WORKSPACE_ID")
    pane = validate_id(env.get("HERDR_PANE_ID"), "HERDR_PANE_ID")
    return machine, workspace, pane


def load_sender_identity(
    environ: dict[str, str] | None = None,
    *,
    sender_kind: str | None = None,
    sender_id: str | None = None,
    sender_name: str | None = None,
    session_id: str | None = None,
) -> dict[str, str]:
    """Resolve the signed sender identity for one Herdr session."""
    env = environ if environ is not None else os.environ
    _machine, _workspace, pane = load_identity(env)
    session = session_id or env.get("HERDR_SESSION_ID") or env.get("HERDR_TAB_ID") or pane
    resolved_id = sender_id or env.get("HERDR_SENDER_ID") or env.get("HERDR_AGENT_ID") or pane
    kind = sender_kind or env.get("HERDR_SENDER_KIND") or "agent"
    name = sender_name or env.get("HERDR_SENDER_NAME") or env.get("HERDR_DISPLAY_AGENT") or env.get("HERDR_AGENT")
    identity = {
        "kind": validate_id(kind, "HERDR_SENDER_KIND"),
        "id": validate_id(resolved_id, "HERDR_SENDER_ID"),
        "session_id": validate_id(session, "HERDR_SESSION_ID"),
        "pane_id": pane,
    }
    if name:
        identity["name"] = name[:128]
    return identity


def generate_key() -> bytes:
    return secrets.token_bytes(32)


def write_key(path: Path, key: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, key.hex().encode("ascii") + b"\n")
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def read_key(path: Path) -> bytes:
    data = path.read_bytes().strip()
    if len(data) == 32:
        return data
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RoutingError(f"HMAC key at {path} is unreadable") from exc
    if len(text) != 64:
        raise RoutingError(f"HMAC key at {path} has invalid length")
    try:
        return bytes.fromhex(text)
    except ValueError as exc:
        raise RoutingError(f"HMAC key at {path} is not hex") from exc


def sign(key: bytes, canonical: bytes) -> str:
    return hmac.new(key, canonical, "sha256").hexdigest()


def verify(key: bytes, canonical: bytes, digest: str) -> bool:
    if not isinstance(digest, str) or len(digest) != 64:
        return False
    expected = sign(key, canonical)
    return hmac.compare_digest(expected, digest)
