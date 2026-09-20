#!/usr/bin/env python3
"""Print the current IPC routing matrix from the supervisor snapshot."""

from __future__ import annotations

import sys
import time

from plugin_runtime import herdr_json, load_snapshot, machine_id, snapshot_path
from routing import socket_path


def _age_ms(recv_ns: int | None) -> str:
    if not recv_ns:
        return "-"
    age = max(0, time.time_ns() - int(recv_ns))
    return f"{age / 1_000_000:.0f}ms"


def main() -> int:
    try:
        agents = (herdr_json("agent", "list").get("result") or {}).get("agents") or []
    except Exception as exc:  # noqa: BLE001
        print(f"herdr-ipc: agent list failed: {exc}", file=sys.stderr)
        agents = []
    snap = load_snapshot()
    panes = snap.get("panes") or {}
    mid = machine_id()
    print(f"machine: {mid}")
    print(f"snapshot: {snapshot_path()}")
    print()
    print(f"{'WS':<8} {'SENDER':<24} {'AGENT':<10} {'LIVE':<10} {'IPC':<10} {'SOCK':<28} {'AGE':>8} {'ROUTE'}")
    print("-" * 100)
    leakage = 0
    matched = 0
    missing = 0
    for agent in agents:
        ws = str(agent.get("workspace_id") or "")
        pane = str(agent.get("pane_id") or "")
        live = str(agent.get("agent_status") or "?")
        name = str(agent.get("agent") or "?")
        rec = panes.get(pane) or {}
        sender = rec.get("sender") or {}
        sender_label = f"{sender.get('kind', '?')}:{sender.get('name') or sender.get('id') or pane}"[:24]
        ipc = str(rec.get("status") or "-")
        sock = str(socket_path(mid, ws).name) if ws else "-"
        route = "PASS"
        if not rec:
            route = "MISS"
            missing += 1
        elif rec.get("workspace_id") != ws or rec.get("orchestrator_workspace_id") not in (None, ws):
            route = "LEAK"
            leakage += 1
        else:
            matched += 1
        print(
            f"{ws:<8} {sender_label:<24} {name:<10} {live:<10} {ipc:<10} {sock:<28} {_age_ms(rec.get('t_recv_ns')):>8} {route}"
        )
    print("-" * 100)
    print(f"matched={matched} missing={missing} leakage={leakage} live_agents={len(agents)}")
    return 0 if leakage == 0 and missing == 0 and agents else 1


if __name__ == "__main__":
    raise SystemExit(main())
