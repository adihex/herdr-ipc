# herdr-ipc

Workspace-scoped Unix-socket IPC for Herdr pane workers.

This directory is two packages in one tree:

| Manifest | Spec | Who loads it |
|---|---|---|
| `herdr-plugin.toml` | [Herdr Plugins](https://herdr.dev/docs/plugins/) | Herdr host (`local.ipc`) |
| `plugin.json` | [Agent Plugins 1.0.0](https://agent-plugins.org/specification) | Agent clients (skills + optional MCP later) |

Lifecycle (`idle` / `working` / `blocked` / `done`) stays on Herdr. IPC carries extra payloads (blocker reason, artifact, handoff).

The socket is push-based; it does not use a polling loop. Fire-and-forget hooks
send once. Diagnostic or interactive clients can pass `--wait-ack` for one
request/reply round trip. The ACK includes a `reply` object when the supervisor
accepts the message, tied to the sender's pane and nonce. Use Herdr's
event-driven agent wait for lifecycle completion.

## Herdr host

```sh
herdr plugin link /path/to/herdr-ipc --enabled
herdr plugin action invoke local.ipc.ingest
herdr plugin action invoke local.ipc.verify
herdr plugin pane open --plugin local.ipc --entrypoint inbox --placement tab
```

Herdr does **not** ship a custom-widget SDK. Plugin v1: “native non-terminal plugin UI are not part of plugin v1.” The supported UI surfaces are:

1. **Plugin pane** (this inbox) — a real terminal. Placements: `overlay`, `tab`, `split`, `zoomed` (and `popup` on newer Herdr). Correct place to dump inbox contents.
2. **Agent sidebar tokens** — `pane report-metadata --token ipc=... --token ipc_note=...`, then add `$ipc` / `$ipc_note` to `[ui.sidebar.agents] rows`. Glanceable, not a full inbox.
3. **`agent.view.set`** — filter/sort the built-in Agents list. Not a new widget.
4. **`pane.graphics.*`** — Kitty images over a pane. Not a widget toolkit.

Optional sidebar rows (you edit `~/.config/herdr/config.toml`):

```toml
[ui.sidebar.agents]
rows = [
  ["state_icon", "agent", "$ipc"],
  ["$ipc_note"],
]
```

## Agent clients

Portable skill: `skills/herdr-ipc/SKILL.md` (Agent Skills spec).

Hooks are **not** a portable Agent Plugins v1 component. They live in client-extension directories (`com.anthropic.claude-code/`, `xai.grok/`, `com.openai.codex/`) and are installed opt-in:

```sh
herdr plugin action invoke local.ipc.install-hooks
```

That writes beside each official Herdr integration (never overwrites `herdr-agent-state`):

| Agent | Config touched |
|---|---|
| Grok | `~/.grok/hooks/herdr-ipc.json` (sibling file; Grok merges `hooks/*.json`) |
| Claude | `~/.claude/settings.json` extra SessionStart/Stop/PostToolUseFailure group |
| Codex | `~/.codex/hooks.json` extra SessionStart/Stop/PermissionRequest group |
| Devin | `~/.config/devin/config.json` extra hook groups |
| Droid | `~/.factory/settings.json` `hooks` key |
| Pi | `~/.pi/agent/extensions/herdr-ipc.ts` (sibling of `herdr-agent-state.ts`) |

The hook is a no-op unless `HERDR_ENV=1`. It never prints to stdout and never calls `pane.report-agent`.

Manual push from a pane:

```sh
./herdr-push-hook.sh blocked '{"reason":"need API key"}'
```

## Isolation

```
/tmp/herdr_${HERDR_MACHINE_ID:-local}_${HERDR_WORKSPACE_ID}.sock
```

HMAC-SHA256 per socket. `python3 test_matrix_isolation.py` is the synthetic proof; `local.ipc.verify` is the live-session proof.
