#!/usr/bin/env python3
"""Snapshot every live Herdr agent onto its workspace-scoped IPC socket."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from ipc_client import PushError, push
from plugin_runtime import ensure_supervisor, herdr_json, machine_id, report_ipc_tokens
from protocol import ProtocolError, normalize_status
from routing import RoutingError


def _agents() -> list[dict[str, Any]]:
    payload = herdr_json("agent", "list")
    agents = (payload.get("result") or {}).get("agents") or []
    if not isinstance(agents, list):
        return []
    return [a for a in agents if isinstance(a, dict)]


def ingest_agents() -> dict[str, Any]:
    mid = machine_id()
    sent = 0
    errors: list[str] = []
    by_ws: dict[str, int] = {}
    for agent in _agents():
        workspace = str(agent.get("workspace_id") or "")
        pane = str(agent.get("pane_id") or "")
        status = str(agent.get("agent_status") or "unknown")
        env = os.environ.copy()
        env["HERDR_MACHINE_ID"] = mid
        env["HERDR_WORKSPACE_ID"] = workspace
        env["HERDR_PANE_ID"] = pane
        try:
            normalize_status(status)
            push(
                status,
                {
                    "source": "herdr-agent-list",
                    "agent": agent.get("agent"),
                    "display_agent": agent.get("display_agent"),
                    "title": agent.get("terminal_title_stripped") or agent.get("terminal_title"),
                    "cwd": agent.get("cwd"),
                    "tab_id": agent.get("tab_id"),
                },
                sender_kind="agent",
                sender_id=pane,
                sender_name=str(agent.get("display_agent") or agent.get("agent") or pane),
                session_id=str(agent.get("session_id") or agent.get("tab_id") or pane),
                environ=env,
                wait_ack=True,
                timeout_s=0.5,
            )
            sent += 1
            by_ws[workspace] = by_ws.get(workspace, 0) + 1
            note = str(agent.get("agent") or "")
            report_ipc_tokens(pane, status, note)
        except (PushError, RoutingError, ProtocolError, KeyError) as exc:
            errors.append(f"{workspace}/{pane}: {exc}")
    return {"sent": sent, "errors": errors, "by_workspace": by_ws, "machine_id": mid}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest live Herdr agents into scoped IPC sockets")
    parser.add_argument("--ensure-only", action="store_true", help="start supervisor, do not ingest")
    args = parser.parse_args(argv)
    try:
        pid = ensure_supervisor()
    except Exception as exc:  # noqa: BLE001
        print(f"herdr-ipc: supervisor failed: {exc}", file=sys.stderr)
        return 1
    if args.ensure_only:
        print(json.dumps({"ok": True, "supervisor_pid": pid}, separators=(",", ":")))
        return 0
    report = ingest_agents()
    report["ok"] = not report["errors"]
    report["supervisor_pid"] = pid
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
