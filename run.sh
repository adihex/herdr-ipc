#!/bin/sh
# Startup hook: ensure the IPC supervisor is running, ingest live agents, exit.
# Herdr startup commands are one-shot; the supervisor is the long-lived daemon.
set -u
cd "$(dirname "$0")"
PYTHON="${HERDR_IPC_PYTHON:-python3}"
"$PYTHON" ingest.py
exit 0
