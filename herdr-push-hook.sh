#!/usr/bin/env bash
# herdr-push-hook.sh — drop a status envelope on the scoped Herdr Unix socket.
#
# Intended for Claude Code / Codex / Devin / other pane-local tools:
#   herdr-push-hook.sh working '{"task":"compile"}'
#   herdr-push-hook.sh blocked '{"error":"test failed"}'
#   echo '{"reason":"file saved"}' | herdr-push-hook.sh done
#
# Routing is derived ONLY from Herdr-injected invariants:
#   HERDR_MACHINE_ID, HERDR_WORKSPACE_ID, HERDR_PANE_ID
# Socket: ${HERDR_IPC_SOCKET_DIR:-/tmp}/herdr_${HERDR_MACHINE_ID}_${HERDR_WORKSPACE_ID}.sock
#
# The Python client is non-blocking: it writes the envelope and returns. It
# never waits for orchestrator-side processing. Connect+send is capped so a
# missing daemon cannot stall the worker.

set -u

HOOK_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
STATUS="${1:-${HERDR_STATUS:-working}}"
PAYLOAD="${2:-}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "herdr-push-hook: python3 is required" >&2
  exit 127
fi

: "${HERDR_MACHINE_ID:=local}"
export HERDR_MACHINE_ID
for var in HERDR_WORKSPACE_ID HERDR_PANE_ID; do
  eval "val=\${$var:-}"
  if [ -z "$val" ]; then
    echo "herdr-push-hook: $var is not set" >&2
    exit 2
  fi
done

export PYTHONPATH="${HOOK_DIR}${PYTHONPATH:+:$PYTHONPATH}"

cmd=(python3 "${HOOK_DIR}/ipc_client.py" --status "$STATUS")
if [ -n "${HERDR_IPC_SOCKET_DIR:-}" ]; then
  cmd+=(--socket-dir "$HERDR_IPC_SOCKET_DIR")
fi
if [ -n "$PAYLOAD" ]; then
  cmd+=(--payload "$PAYLOAD")
fi
if [ "${HERDR_IPC_WAIT_ACK:-0}" = "1" ]; then
  cmd+=(--wait-ack)
fi
if [ -n "${HERDR_IPC_TIMEOUT_MS:-}" ]; then
  cmd+=(--timeout-ms "$HERDR_IPC_TIMEOUT_MS")
fi

# If stdin is a pipe, ipc_client.py merges that JSON into the payload.
exec "${cmd[@]}"
