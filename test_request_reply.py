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
            assert reply["nonce"] == result.nonce
            assert len(received) == 1
        finally:
            daemon.stop()


if __name__ == "__main__":
    test_request_reply_round_trip()
    print("request/reply round trip: PASS")
