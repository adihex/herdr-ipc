// herdr-ipc — extra payload only. Do not edit herdr-agent-state.ts.
// no-op unless HERDR_ENV=1. Never reports pane.report_agent.

import { spawnSync } from "node:child_process";

const HOOK = process.env.HERDR_IPC_HOOK || "__HERDR_IPC_HOOK__";

function fire(name: string): void {
  if (process.env.HERDR_ENV !== "1") {
    return;
  }
  if (!process.env.HERDR_WORKSPACE_ID || !process.env.HERDR_PANE_ID) {
    return;
  }
  try {
    spawnSync("/bin/sh", [HOOK], {
      input: JSON.stringify({ hook_event_name: name }),
      stdio: ["pipe", "ignore", "ignore"],
      timeout: 2000,
      env: process.env,
    });
  } catch {
    // ignore — never block Pi
  }
}

export default function (pi: { on: (event: string, fn: (...args: unknown[]) => void) => void }): void {
  pi.on("session_start", () => fire("SessionStart"));
  pi.on("agent_start", () => fire("UserPromptSubmit"));
  pi.on("agent_settled", () => fire("Stop"));
}
