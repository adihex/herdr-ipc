#!/usr/bin/env python3
"""Install herdr-ipc agent-client hooks beside official Herdr integrations.

Never edits herdr-agent-state.ts / herdr.json owned by `herdr integration install`.
Skips an agent when its config directory is missing.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

from plugin_runtime import PLUGIN_ROOT

HOOK_SH = PLUGIN_ROOT / "hooks" / "agent_hook.sh"
PI_TEMPLATE = PLUGIN_ROOT / "hooks" / "herdr-ipc.pi.ts"
MARKER = "herdr-ipc-agent"

CLAUDE_EVENTS = ("SessionStart", "Stop", "PostToolUseFailure")
CODEX_EVENTS = ("SessionStart", "Stop", "PermissionRequest")
DEVIN_EVENTS = ("SessionStart", "Stop", "SessionEnd", "PermissionRequest", "PostToolUse")
DROID_EVENTS = ("SessionStart", "SessionEnd", "PostToolUseFailure", "Notification")
GROK_EVENTS = ("SessionStart", "Stop")
CLAUDE_SESSION_MATCHER = "startup|resume|clear|compact|fork"


def _home() -> Path:
    return Path.home()


def _write_executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _wrapper(path: Path) -> Path:
    _write_executable(path, f"#!/bin/sh\nexec /bin/sh \"{HOOK_SH}\"\n")
    return path


def _command(wrapper: Path) -> str:
    return f'/bin/sh "{wrapper}"'


def _atomic_write_json(path: Path, data: Any) -> None:
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    tmp = path.with_name(path.name + ".herdr-ipc.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _upsert_events(
    hooks_obj: dict[str, Any],
    events: tuple[str, ...],
    command: str,
    *,
    session_start_matcher: str | None = None,
) -> None:
    group_base = {"hooks": [{"type": "command", "command": command, "timeout": 5}]}
    for event in events:
        group = dict(group_base)
        if event == "SessionStart" and session_start_matcher:
            group["matcher"] = session_start_matcher
        existing = hooks_obj.get(event)
        kept: list[Any] = []
        if isinstance(existing, list):
            for item in existing:
                if MARKER not in json.dumps(item, ensure_ascii=False):
                    kept.append(item)
        kept.append(group)
        hooks_obj[event] = kept


def _merge_settings_hooks(
    path: Path,
    events: tuple[str, ...],
    command: str,
    *,
    session_start_matcher: str | None = None,
) -> str:
    if not path.is_file():
        return "skipped (config missing)"
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return "skipped (config not an object)"
    hooks = data.get("hooks")
    if not isinstance(hooks, dict):
        hooks = {}
        data["hooks"] = hooks
    _upsert_events(hooks, events, command, session_start_matcher=session_start_matcher)
    _atomic_write_json(path, data)
    return "installed"


def install_grok() -> dict[str, str]:
    home = Path(os.environ.get("GROK_HOME") or _home() / ".grok")
    if not home.is_dir():
        return {"agent": "grok", "status": "skipped (GROK_HOME missing)"}
    wrapper = _wrapper(home / "hooks" / "herdr-ipc-agent.sh")
    dest = home / "hooks" / "herdr-ipc.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"hooks": {}}
    _upsert_events(payload["hooks"], GROK_EVENTS, _command(wrapper))
    dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return {"agent": "grok", "status": "installed", "path": str(dest)}


def install_claude() -> dict[str, str]:
    home = Path(os.environ.get("CLAUDE_CONFIG_DIR") or _home() / ".claude")
    settings = home / "settings.json"
    if not settings.is_file():
        return {"agent": "claude", "status": "skipped (~/.claude/settings.json missing)"}
    wrapper = _wrapper(home / "hooks" / "herdr-ipc-agent.sh")
    status = _merge_settings_hooks(
        settings,
        CLAUDE_EVENTS,
        _command(wrapper),
        session_start_matcher=CLAUDE_SESSION_MATCHER,
    )
    return {"agent": "claude", "status": status, "path": str(settings)}


def install_codex() -> dict[str, str]:
    home = Path(os.environ.get("CODEX_HOME") or _home() / ".codex")
    hooks = home / "hooks.json"
    if not home.is_dir():
        return {"agent": "codex", "status": "skipped (CODEX_HOME missing)"}
    wrapper = _wrapper(home / "herdr-ipc-agent.sh")
    if not hooks.is_file():
        _atomic_write_json(hooks, {"hooks": {}})
    status = _merge_settings_hooks(hooks, CODEX_EVENTS, _command(wrapper))
    return {"agent": "codex", "status": status, "path": str(hooks)}


def install_devin() -> dict[str, str]:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    home = Path(xdg) / "devin" if xdg else _home() / ".config" / "devin"
    config = home / "config.json"
    if not config.is_file():
        return {"agent": "devin", "status": "skipped (~/.config/devin/config.json missing)"}
    wrapper = _wrapper(home / "herdr-ipc-agent.sh")
    status = _merge_settings_hooks(config, DEVIN_EVENTS, _command(wrapper))
    return {"agent": "devin", "status": status, "path": str(config)}


def install_droid() -> dict[str, str]:
    home = _home() / ".factory"
    settings = home / "settings.json"
    if not settings.is_file():
        return {"agent": "droid", "status": "skipped (~/.factory/settings.json missing)"}
    wrapper = _wrapper(home / "hooks" / "herdr-ipc-agent.sh")
    status = _merge_settings_hooks(settings, DROID_EVENTS, _command(wrapper))
    return {"agent": "droid", "status": status, "path": str(settings)}


def install_pi() -> dict[str, str]:
    base = Path(os.environ.get("PI_CODING_AGENT_DIR") or _home() / ".pi" / "agent")
    ext_dir = base / "extensions"
    if not ext_dir.is_dir():
        return {"agent": "pi", "status": "skipped (~/.pi/agent/extensions missing)"}
    dest = ext_dir / "herdr-ipc.ts"
    template = PI_TEMPLATE.read_text(encoding="utf-8")
    dest.write_text(template.replace("__HERDR_IPC_HOOK__", str(HOOK_SH)), encoding="utf-8")
    return {"agent": "pi", "status": "installed", "path": str(dest)}


def main() -> int:
    results = [
        install_grok(),
        install_claude(),
        install_codex(),
        install_devin(),
        install_droid(),
        install_pi(),
    ]
    print(json.dumps({"ok": True, "script": str(HOOK_SH), "agents": results}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
