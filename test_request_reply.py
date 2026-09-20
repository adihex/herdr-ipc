from __future__ import annotations

import os
import tempfile

from ipc_client import push
from orchestrator_daemon import OrchestratorDaemon


def test_request_reply_round_trip() -> None:
    with tempfile.TemporaryDirectory(prefix="hipc-", dir="/tmp") as socket_dir:
        received: list[dict[str, object]] = []

        def on_message(record: dict[str, object]) -> dict[str, object]:
            received.append(record)
            return {
                "received": True,
                "workspace_id": record["workspace_id"],
                "pane_id": record["pane_id"],
                "sender": record["sender"],
                "nonce": record["nonce"],
            }

        daemon = OrchestratorDaemon("machine_reply", "workspace_reply", socket_dir, on_message=on_message)
        daemon.start()
        try:
            env = os.environ.copy()
            env.update(
                {
                    "HERDR_MACHINE_ID": "machine_reply",
                    "HERDR_WORKSPACE_ID": "workspace_reply",
                    "HERDR_PANE_ID": "pane_reply",
                    "HERDR_SESSION_ID": "agent-session",
                    "HERDR_IPC_SOCKET_DIR": socket_dir,
                }
            )
            result = push(
                "working",
                {"artifact": "report.json"},
                environ=env,
                socket_dir=socket_dir,
                wait_ack=True,
            )
            assert result.ok
            assert result.ack is not None
            reply = result.ack["reply"]
            assert reply["received"] is True
            assert reply["workspace_id"] == "workspace_reply"
            assert reply["pane_id"] == "pane_reply"
            assert reply["sender"]["kind"] == "agent"
            assert reply["sender"]["id"] == "pane_reply"
            assert reply["sender"]["session_id"] == "agent-session"
            assert reply["nonce"] == result.nonce
            assert len(received) == 1

            user = push(
                "working",
                {"source": "manual-user-message"},
                environ=env,
                socket_dir=socket_dir,
                wait_ack=True,
                sender_kind="user",
                sender_id="aditya",
                sender_name="Aditya",
                session_id="user-session",
            )
            assert user.ok and user.ack is not None
            assert user.ack["reply"]["sender"] == {
                "kind": "user",
                "id": "aditya",
                "name": "Aditya",
                "session_id": "user-session",
                "pane_id": "pane_reply",
            }
            assert daemon.identity_snapshot()["user-session"]["kind"] == "user"
            assert len(received) == 2
        finally:
            daemon.stop()


if __name__ == "__main__":
    test_request_reply_round_trip()
    print("request/reply round trip: PASS")
