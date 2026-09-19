#!/bin/sh
# Fire-and-forget IPC push for Claude / Codex / Grok lifecycle hooks.
# Must print nothing on stdout (Claude SessionStart injects stdout into context).
[ "${HERDR_ENV:-}" = "1" ] || { { command -p cat 2>/dev/null || cat; } >/dev/null 2>&1 || :; exit 0; }
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$DIR/.." && pwd)
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export HERDR_MACHINE_ID="${HERDR_MACHINE_ID:-local}"
exec python3 "$DIR/agent_hook.py"
