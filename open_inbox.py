#!/usr/bin/env python3
"""Open the inbox as a Herdr plugin pane (tab by default)."""

from __future__ import annotations

import os
import sys

from plugin_runtime import herdr_bin


def main() -> int:
    placement = os.environ.get("HERDR_IPC_INBOX_PLACEMENT", "tab")
    cmd = [
        herdr_bin(),
        "plugin",
        "pane",
        "open",
        "--plugin",
        "local.ipc",
        "--entrypoint",
        "inbox",
        "--placement",
        placement,
        "--focus",
    ]
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    raise SystemExit(main())
