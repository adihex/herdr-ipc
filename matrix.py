#!/usr/bin/env python3
"""Live plugin pane: routing matrix for every worker Herdr currently sees."""

from __future__ import annotations

import os
import time

from plugin_runtime import ensure_supervisor
from status import main as print_status


def main() -> int:
    try:
        ensure_supervisor()
    except Exception as exc:  # noqa: BLE001
        print(f"supervisor: {exc}")
    print("herdr-ipc matrix  (Ctrl-C to close)\n")
    try:
        while True:
            if os.environ.get("TERM"):
                print("\033[H\033[2J", end="")
            print_status()
            time.sleep(0.75)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
