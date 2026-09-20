#!/usr/bin/env python3
"""Herdr event hook: route pane.agent_status_changed onto the scoped socket."""

from __future__ import annotations

import json
import os
import sys
from typing import Any

from ipc_client import PushError, push
from plugin_runtime import ensure_supervisor, machine_id, report_ipc_tokens
from protocol import ProtocolError
from routing import RoutingError


def _event_payload() -> dict[str, Any]:
    raw = os.environ.get("HERDR_PLUGIN_EVENT_JSON") or "{}"
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(value, dict):
        return {}
    data = value.get("data")
    if isinstance(data, dict):
        merged = dict(data)
        merged.setdefault("event", value.get("event") or value.get("type"))
        return merged
    return value


def main() -> int:
    event = _event_payload()
    workspace = str(event.get("workspace_id") or os.environ.get("HERDR_WORKSPACE_ID") or "")
    pane = str(event.get("pane_id") or os.environ.get("HERDR_PANE_ID") or "")
    status = str(event.get("agent_status") or event.get("status") or "unknown")
    if not workspace or not pane:
        return 0
    try:
        ensure_supervisor()
        env = os.environ.copy()
        env["HERDR_MACHINE_ID"] = machine_id()
        env["HERDR_WORKSPACE_ID"] = workspace
        env["HERDR_PANE_ID"] = pane
        push(
            status,
            {
                "source": "pane.agent_status_changed",
                "agent": event.get("agent"),
                "display_agent": event.get("display_agent"),
                "title": event.get("title"),
                "event": event.get("event"),
            },
            sender_kind="agent",
            sender_id=pane,
            sender_name=str(event.get("display_agent") or event.get("agent") or pane),
            session_id=str(event.get("session_id") or event.get("tab_id") or pane),
            environ=env,
            wait_ack=False,
            timeout_s=0.2,
        )
        note = str(event.get("title") or event.get("display_agent") or event.get("agent") or "")
        report_ipc_tokens(pane, status, note)
    except (PushError, RoutingError, ProtocolError, TimeoutError, RuntimeError) as exc:
        print(f"herdr-ipc event: {exc}", file=sys.stderr)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
