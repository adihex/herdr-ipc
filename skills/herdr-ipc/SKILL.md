---
name: herdr-ipc
description: >
  Push extra payloads on the workspace-scoped Herdr Unix socket and inspect
  sibling inboxes. Use when HERDR_ENV=1 and you need to hand off an artifact,
  report a blocker reason, or wait on another pane in this workspace. Do not
  use for idle/working/blocked lifecycle — Herdr already owns that.
compatibility: Requires a Herdr pane (HERDR_ENV=1) and python3.
---

# herdr-ipc

You are inside a Herdr pane when `HERDR_ENV=1`.

## Lifecycle vs extra payload

Do **not** report `idle` / `working` / `blocked` / `done` yourself. Herdr already
classifies that from screen manifests or official integrations
(`herdr pane report-agent`). Dual-writing lifecycle onto the IPC socket fights
`herdr agent wait` and the sidebar.

Use IPC only for payloads Herdr does not own: blocker reason, artifact path,
handoff packet, URL, test output.

## Push

If `HERDR_WORKSPACE_ID` or `HERDR_PANE_ID` is unset, stop. You are not in Herdr.

From this skill directory:

```bash
../../herdr-push-hook.sh blocked '{"reason":"waiting on API key","artifact":""}'
../../herdr-push-hook.sh done '{"artifact":"path/or/url","summary":"one line"}'
```

`HERDR_MACHINE_ID` defaults to `local`. The socket is

`/tmp/herdr_${HERDR_MACHINE_ID:-local}_${HERDR_WORKSPACE_ID}.sock`

The hook is fire-and-forget. Do not wait on the orchestrator. Do not invent
another socket path.

## Wait on siblings

Use Herdr, not a poll loop:

```bash
"$HERDR_BIN_PATH" agent wait <pane_id> --until done
"$HERDR_BIN_PATH" agent list
```

To read extra payloads, open the inbox pane or read the snapshot the supervisor
writes (Herdr plugin action `local.ipc.status`).

## Do not

- Call `herdr pane report-agent` for this plugin (that steals lifecycle authority).
- Poll `herdr agent list` in a loop.
- Push when `HERDR_ENV` is not `1`.
