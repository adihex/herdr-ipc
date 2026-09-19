"""Shared Herdr plugin runtime: state dir, supervisor lifecycle, CLI helpers."""

from __future__ import annotations

import json
import os
import signal
import socket as socklib
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from routing import DEFAULT_MACHINE_ID, socket_path, validate_id

PLUGIN_ROOT = Path(__file__).resolve().parent


def state_dir() -> Path:
    raw = os.environ.get("HERDR_PLUGIN_STATE_DIR")
    path = Path(raw) if raw else Path("/tmp/herdr-ipc-state")
    path.mkdir(parents=True, exist_ok=True)
    return path


def machine_id(environ: dict[str, str] | None = None) -> str:
    env = environ if environ is not None else os.environ
    return validate_id(env.get("HERDR_MACHINE_ID") or DEFAULT_MACHINE_ID, "HERDR_MACHINE_ID")


def herdr_bin() -> str:
    return os.environ.get("HERDR_BIN_PATH") or os.environ.get("HERDR") or "herdr"


def report_ipc_tokens(pane_id: str, status: str, note: str = "") -> None:
    """Display-only sidebar tokens. Never takes lifecycle authority."""
    if not pane_id:
        return
    args = [
        "pane",
        "report-metadata",
        pane_id,
        "--source",
        "plugin:local.ipc",
        "--token",
        f"ipc={status[:80]}",
    ]
    if note:
        args.extend(["--token", f"ipc_note={note[:80]}"])
    subprocess.run(
        [herdr_bin(), *args],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )


def herdr_json(*args: str, timeout: float = 15.0) -> dict[str, Any]:
    result = subprocess.run(
        [herdr_bin(), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"herdr {' '.join(args)} failed")
    return json.loads(result.stdout)


def pid_path() -> Path:
    return state_dir() / "supervisor.pid"


def ready_path() -> Path:
    return state_dir() / "supervisor.ready"


def snapshot_path() -> Path:
    return state_dir() / "snapshot.json"


def log_path() -> Path:
    return state_dir() / "supervisor.log"


def jsonl_path() -> Path:
    return state_dir() / "inbox.jsonl"


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def supervisor_pid() -> int | None:
    path = pid_path()
    if not path.is_file():
        return None
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None
    return pid if pid_alive(pid) else None


def _socket_live() -> bool:
    try:
        workspace = os.environ.get("HERDR_WORKSPACE_ID")
        if not workspace:
            payload = herdr_json("workspace", "list")
            rows = (payload.get("result") or {}).get("workspaces") or []
            workspace = rows[0]["workspace_id"] if rows else None
        if not workspace:
            return False
        path = socket_path(machine_id(), str(workspace))
        probe = socklib.socket(socklib.AF_UNIX, socklib.SOCK_STREAM)
        try:
            probe.settimeout(0.05)
            probe.connect(str(path))
        finally:
            probe.close()
        return True
    except OSError:
        return False


def ensure_supervisor(timeout: float = 8.0) -> int:
    existing = supervisor_pid()
    if existing is not None:
        if ready_path().is_file() or _socket_live():
            try:
                os.kill(existing, signal.SIGUSR1)
            except OSError:
                pass
            return existing
    if _socket_live() and existing is None:
        # Another plugin invocation already bound the sockets.
        return 0
    lock = state_dir() / "supervisor.lock"
    lock_fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        import fcntl

        fcntl.flock(lock_fd, fcntl.LOCK_EX)
    except OSError:
        pass
    existing = supervisor_pid()
    if existing is not None and (ready_path().is_file() or _socket_live()):
        return existing
    if ready_path().exists():
        try:
            ready_path().unlink()
        except FileNotFoundError:
            pass
    log = log_path().open("ab")
    proc = subprocess.Popen(
        [sys.executable, str(PLUGIN_ROOT / "supervisor.py")],
        cwd=str(PLUGIN_ROOT),
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=os.environ.copy(),
    )
    pid_path().write_text(str(proc.pid), encoding="utf-8")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready_path().is_file() and pid_alive(proc.pid):
            return proc.pid
        if proc.poll() is not None:
            tail = log_path().read_text(encoding="utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"supervisor exited {proc.returncode}\n{tail}")
        time.sleep(0.05)
    raise TimeoutError("supervisor did not become ready")


def load_snapshot() -> dict[str, Any]:
    path = snapshot_path()
    if not path.is_file():
        return {"panes": {}, "updated_ns": 0}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"panes": {}, "updated_ns": 0}
    if not isinstance(data, dict):
        return {"panes": {}, "updated_ns": 0}
    data.setdefault("panes", {})
    return data
