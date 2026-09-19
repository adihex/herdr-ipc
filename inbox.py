#!/usr/bin/env python3
"""Inbox viewer: latest envelope per pane plus recent JSONL payloads.

Herdr has no custom-widget SDK (plugin v1: native non-terminal UI is out of
scope). This is a plugin pane — a real terminal Herdr owns.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from plugin_runtime import ensure_supervisor, jsonl_path, load_snapshot, snapshot_path


def _tail_jsonl(path: Path, limit: int = 24) -> list[dict]:
    if not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    out: list[dict] = []
    for line in lines[-limit:]:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            out.append(rec)
    return out


def _payload_note(payload: dict) -> str:
    if not payload:
        return ""
    for key in ("reason", "summary", "artifact", "title", "hook_event", "source", "agent"):
        value = payload.get(key)
        if value:
            return str(value)[:56]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)[:56]


def render() -> str:
    snap = load_snapshot()
    panes = snap.get("panes") or {}
    rows = sorted(panes.values(), key=lambda r: (str(r.get("workspace_id")), str(r.get("pane_id"))))
    lines = [
        "HERDR IPC INBOX   Ctrl-C closes the pane",
        f"snapshot: {snapshot_path()}",
        "",
        f"{'WS':<8} {'PANE':<12} {'STATUS':<10} {'NOTE':<56}",
        "-" * 90,
    ]
    if not rows:
        lines.append("(empty — run: herdr plugin action invoke local.ipc.ingest)")
    for rec in rows:
        note = _payload_note(rec.get("payload") or {})
        lines.append(
            f"{str(rec.get('workspace_id') or '-'):<8} "
            f"{str(rec.get('pane_id') or '-'):<12} "
            f"{str(rec.get('status') or '-'):<10} "
            f"{note:<56}"
        )
    recent = _tail_jsonl(jsonl_path())
    lines.extend(["", f"recent payloads ({len(recent)})", "-" * 90])
    for rec in recent[-12:]:
        note = _payload_note(rec.get("payload") or {})
        lines.append(
            f"{str(rec.get('workspace_id') or '-'):<8} "
            f"{str(rec.get('pane_id') or '-'):<12} "
            f"{str(rec.get('status') or '-'):<10} "
            f"{note}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    try:
        ensure_supervisor()
    except Exception as exc:  # noqa: BLE001
        print(f"supervisor: {exc}")
    path = snapshot_path()
    last = 0.0
    try:
        while True:
            mtime = path.stat().st_mtime if path.is_file() else 0.0
            if mtime != last:
                last = mtime
                if os.environ.get("TERM"):
                    print("\033[H\033[2J", end="")
                print(render(), flush=True)
            time.sleep(0.4)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
