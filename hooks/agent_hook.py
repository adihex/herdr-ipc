#!/usr/bin/env python3
"""Deterministic agent-client hook. No-op outside Herdr. Never writes stdout."""

from __future__ import annotations

import json
import os
import sys
from typing import Any

# Drain stdin even when we no-op, so the host does not block on a pipe.
_raw = sys.stdin.read() if not sys.stdin.isatty() else ""


def _map_status(event: str) -> str:
    key = (event or "").lower()
    if key in {"stop", "sessionend", "taskcompleted"}:
        return "done"
    if key in {"posttoolusefailure", "stopfailure", "notification"}:
        return "blocked"
    if key in {"sessionstart", "userpromptsubmit", "pretooluse", "posttooluse", "subagentstart"}:
        return "working"
    return "working"


def main() -> int:
    if os.environ.get("HERDR_ENV") != "1":
        return 0
    if not os.environ.get("HERDR_WORKSPACE_ID") or not os.environ.get("HERDR_PANE_ID"):
        return 0
    event: dict[str, Any] = {}
    if _raw.strip():
        try:
            parsed = json.loads(_raw)
            if isinstance(parsed, dict):
                event = parsed
        except json.JSONDecodeError:
            event = {}
    name = str(event.get("hook_event_name") or event.get("event") or os.environ.get("HERDR_PLUGIN_EVENT") or "working")
    status = _map_status(name)
    payload = {
        "source": "agent-hook",
        "hook_event": name,
        "tool_name": event.get("tool_name"),
    }
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, root)
    try:
        from ipc_client import push
        from plugin_runtime import ensure_supervisor, report_ipc_tokens
    except Exception:
        return 0
    try:
        ensure_supervisor()
        push(status, payload, wait_ack=False, timeout_s=0.2)
        report_ipc_tokens(os.environ.get("HERDR_PANE_ID") or "", status, name)
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
