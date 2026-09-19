#!/usr/bin/env python3
"""Ultimate check: every live Herdr worker routes to the correct workspace socket."""

from __future__ import annotations

import sys
import time

from ingest import ingest_agents
from plugin_runtime import ensure_supervisor, herdr_json, load_snapshot, machine_id
from routing import socket_path


def main() -> int:
    pid = ensure_supervisor()
    report = ingest_agents()
    time.sleep(0.2)
    agents = (herdr_json("agent", "list").get("result") or {}).get("agents") or []
    snap = load_snapshot()
    panes = snap.get("panes") or {}
    mid = machine_id()

    print("=" * 96)
    print("HERDR LIVE IPC MATRIX")
    print("=" * 96)
    print(f"supervisor_pid: {pid}")
    print(f"machine_id:     {mid}")
    print(f"ingest_sent:    {report['sent']}")
    print()
    print(f"{'WS':<8} {'PANE':<12} {'AGENT':<10} {'STATUS':<10} {'SOCKET':<32} {'RESULT'}")
    print("-" * 96)

    leakage = 0
    missing = 0
    matched = 0
    by_ws: dict[str, list[str]] = {}
    for agent in agents:
        ws = str(agent.get("workspace_id") or "")
        pane = str(agent.get("pane_id") or "")
        name = str(agent.get("agent") or "?")
        status = str(agent.get("agent_status") or "?")
        rec = panes.get(pane) or {}
        sock = socket_path(mid, ws).name
        by_ws.setdefault(ws, [])
        result = "PASS"
        if not rec:
            result = "MISS"
            missing += 1
        elif rec.get("machine_id") != mid:
            result = "LEAK-MACHINE"
            leakage += 1
        elif rec.get("workspace_id") != ws:
            result = "LEAK-WORKSPACE"
            leakage += 1
        elif rec.get("orchestrator_workspace_id") not in (None, ws):
            result = "LEAK-ORCH"
            leakage += 1
        else:
            matched += 1
            by_ws[ws].append(pane)
        print(f"{ws:<8} {pane:<12} {name:<10} {status:<10} {sock:<32} {result}")

    # Cross-inbox: a pane recorded under a workspace it does not belong to.
    for pane, rec in panes.items():
        owner = None
        for agent in agents:
            if agent.get("pane_id") == pane:
                owner = agent.get("workspace_id")
                break
        if owner and rec.get("workspace_id") not in (None, owner):
            leakage += 1

    print("-" * 96)
    print(
        f"ASSERT 100% delivery: {matched}/{len(agents)}  "
        f"missing={missing}  leakage={leakage}  "
        f"workspaces={list(by_ws)}"
    )
    if report["errors"]:
        print("INGEST ERRORS")
        for err in report["errors"]:
            print(f"  - {err}")
    ok = leakage == 0 and missing == 0 and len(agents) > 0 and not report["errors"]
    print("ALL LIVE ASSERTIONS PASSED" if ok else "LIVE ASSERTIONS FAILED")
    print("=" * 96)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
