#!/usr/bin/env python3
"""Long-lived plugin supervisor: one orchestrator daemon per workspace."""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from orchestrator_daemon import OrchestratorDaemon
from plugin_runtime import (
    jsonl_path,
    load_snapshot,
    log_path,
    machine_id,
    pid_path,
    ready_path,
    snapshot_path,
    state_dir,
    herdr_json,
)
from routing import RoutingError, validate_id

_stop = threading.Event()
_lock = threading.Lock()
_daemons: dict[str, OrchestratorDaemon] = {}
_snapshot: dict[str, Any] = {"panes": {}, "sessions": {}, "updated_ns": 0}


def _workspaces() -> list[str]:
    found: list[str] = []
    try:
        payload = herdr_json("workspace", "list")
        for row in (payload.get("result") or {}).get("workspaces") or []:
            ws = row.get("workspace_id")
            if ws:
                found.append(validate_id(str(ws), "HERDR_WORKSPACE_ID"))
    except Exception as exc:  # noqa: BLE001
        print(f"workspace list failed: {exc}", file=sys.stderr, flush=True)
    current = os.environ.get("HERDR_WORKSPACE_ID")
    if current:
        try:
            found.append(validate_id(current, "HERDR_WORKSPACE_ID"))
        except RoutingError:
            pass
    # unique, stable order
    return list(dict.fromkeys(found))


def _persist() -> None:
    path = snapshot_path()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _on_message(record: dict[str, Any]) -> dict[str, Any]:
    pane = record.get("pane_id")
    if not pane:
        return {"received": False, "reason": "missing_pane_id"}
    slim = {
        "machine_id": record.get("machine_id"),
        "workspace_id": record.get("workspace_id"),
        "pane_id": pane,
        "session_id": record.get("session_id"),
        "socket_id": record.get("socket_id"),
        "sender": record.get("sender") or {},
        "status": record.get("status"),
        "t_recv_ns": record.get("t_recv_ns"),
        "latency_ns": record.get("latency_ns"),
        "nonce": record.get("nonce"),
        "payload": record.get("payload") or {},
        "orchestrator_workspace_id": record.get("orchestrator_workspace_id"),
    }
    with _lock:
        _snapshot["panes"][pane] = slim
        session = record.get("session_id")
        if session:
            _snapshot.setdefault("sessions", {})[session] = record.get("sender") or {}
        _snapshot["updated_ns"] = time.time_ns()
        _persist()
        with jsonl_path().open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(slim, separators=(",", ":"), ensure_ascii=False) + "\n")
    return {
        "received": True,
        "workspace_id": record.get("workspace_id"),
        "pane_id": pane,
        "session_id": record.get("session_id"),
        "sender": record.get("sender") or {},
        "nonce": record.get("nonce"),
    }


def bind_workspaces() -> None:
    mid = machine_id()
    for ws in _workspaces():
        with _lock:
            if ws in _daemons:
                continue
            daemon = OrchestratorDaemon(mid, ws, on_message=_on_message, ack=True)
            daemon.start()
            _daemons[ws] = daemon
            print(f"bound {daemon.socket_path}", flush=True)


def _handle_stop(_signum: int, _frame: Any) -> None:
    _stop.set()


def _handle_refresh(_signum: int, _frame: Any) -> None:
    try:
        bind_workspaces()
    except Exception as exc:  # noqa: BLE001
        print(f"refresh failed: {exc}", file=sys.stderr, flush=True)


def main() -> int:
    state_dir()
    pid_path().write_text(str(os.getpid()), encoding="utf-8")
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGUSR1, _handle_refresh)
    existing = load_snapshot()
    with _lock:
        _snapshot.update(existing)
        _snapshot.setdefault("panes", {})
        _snapshot.setdefault("sessions", {})
    bind_workspaces()
    if not _daemons:
        # Still listen for the caller's workspace so event hooks have a socket.
        mid = machine_id()
        ws = os.environ.get("HERDR_WORKSPACE_ID") or "w0"
        daemon = OrchestratorDaemon(mid, ws, on_message=_on_message, ack=True)
        daemon.start()
        _daemons[ws] = daemon
        print(f"bound fallback {daemon.socket_path}", flush=True)
    ready_path().write_text("ok\n", encoding="utf-8")
    print(f"ready workspaces={list(_daemons)} log={log_path()}", flush=True)
    while not _stop.wait(timeout=30):
        pass
    for daemon in list(_daemons.values()):
        daemon.stop()
    try:
        ready_path().unlink()
    except FileNotFoundError:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
