#!/usr/bin/env python3
"""End-to-end isolation matrix for Herdr push IPC.

Simulates two machines × two workspaces, fires concurrent worker alerts, and
prints an assertions-based table proving:

  * 100% of honest messages reached the correct orchestrator
  * 0% cross-talk / leakage across machine or workspace boundaries
  * p99 push-delivery latency is under 5 milliseconds
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import queue
import secrets
import shutil
import stat
import subprocess
import sys
import threading
import time
import traceback
import unittest
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ipc_client import PushError, push, push_forged_for_tests  # noqa: E402
from orchestrator_daemon import OrchestratorDaemon, process_main  # noqa: E402
from protocol import build_envelope  # noqa: E402
from routing import (  # noqa: E402
    RoutingError,
    key_path,
    production_socket_path,
    read_key,
    socket_name,
    socket_path,
    validate_id,
)

MACHINES = ("machine_alpha", "machine_beta")
WORKSPACES = ("project_commerce", "project_analytics")
STATUSES = ("working", "blocked", "done")
WORKERS_PER_MATRIX = 12
MESSAGES_PER_WORKER = 25  # 12 * 25 = 300 honest messages / matrix
WARMUP_PER_MATRIX = 16
LATENCY_PROBES = 200  # serial pushes; burst p99 is scheduling, not IPC
LATENCY_BUDGET_NS = 5_000_000  # 5 milliseconds
HOOK = ROOT / "herdr-push-hook.sh"


def matrices() -> list[tuple[str, str]]:
    return [(m, w) for m in MACHINES for w in WORKSPACES]


def key_of(machine: str, workspace: str) -> str:
    return f"{machine}/{workspace}"


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.0005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def percentile(values: list[int], p: float) -> int:
    if not values:
        raise ValueError("no samples")
    ordered = sorted(values)
    rank = max(1, math.ceil((p / 100.0) * len(ordered)))
    return ordered[rank - 1]


def ns_ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f}ms"


@dataclass
class MatrixRow:
    machine: str
    workspace: str
    sent: int
    recv: int
    foreign: int
    rejected: int
    min_ns: int
    median_ns: int
    p99_ns: int
    max_ns: int

    @property
    def delivery_ok(self) -> bool:
        return self.sent == self.recv

    @property
    def leak_ok(self) -> bool:
        return self.foreign == 0

    @property
    def latency_ok(self) -> bool:
        return self.p99_ns < LATENCY_BUDGET_NS

    @property
    def result(self) -> str:
        return "PASS" if self.delivery_ok and self.leak_ok and self.latency_ok else "FAIL"


@dataclass
class MatrixReport:
    socket_dir: str
    rows: list[MatrixRow]
    honest_sent: int
    honest_recv: int
    leakage: int
    attacks_sent: int
    attacks_rejected: int
    attacks_accepted: int
    hook_sent: int
    hook_recv: int
    missing_nonces: list[str] = field(default_factory=list)
    extra_nonces: list[str] = field(default_factory=list)
    path_contract_ok: bool = True
    bind_exclusive_ok: bool = True
    errors: list[str] = field(default_factory=list)

    def assert_all(self) -> None:
        assert self.path_contract_ok, "socket path contract failed"
        assert self.bind_exclusive_ok, "exclusive bind failed"
        assert self.honest_sent > 0, "no honest messages were sent"
        assert self.honest_recv == self.honest_sent, (
            f"delivery {self.honest_recv}/{self.honest_sent}; missing={self.missing_nonces[:8]}"
        )
        assert self.leakage == 0, f"cross-talk detected: {self.leakage} foreign envelopes"
        assert not self.extra_nonces, f"unexpected envelopes: {self.extra_nonces[:8]}"
        assert self.attacks_accepted == 0, f"{self.attacks_accepted} injection attempts were accepted"
        assert self.attacks_rejected == self.attacks_sent, (
            f"rejected {self.attacks_rejected}/{self.attacks_sent} injection attempts"
        )
        assert self.hook_recv == self.hook_sent, f"hook delivery {self.hook_recv}/{self.hook_sent}"
        for row in self.rows:
            assert row.delivery_ok, f"{row.machine}/{row.workspace} dropped messages"
            assert row.leak_ok, f"{row.machine}/{row.workspace} inbox contains foreign mail"
            assert row.latency_ok, (
                f"{row.machine}/{row.workspace} p99 {ns_ms(row.p99_ns)} exceeds 5ms"
            )
        if self.errors:
            raise AssertionError("matrix errors:\n" + "\n".join(self.errors))

    def render(self) -> str:
        lines = [
            "=" * 96,
            "HERDR IPC ISOLATION MATRIX",
            "=" * 96,
            f"socket_dir: {self.socket_dir}",
            "protocol:   AF_UNIX SOCK_STREAM + HMAC-SHA256 (identity-bound)",
            f"matrices:   {len(self.rows)}  ({len(MACHINES)} machines × {len(WORKSPACES)} workspaces)",
            f"honest:     {WORKERS_PER_MATRIX} workers × {MESSAGES_PER_WORKER} pushes × {len(self.rows)} matrices",
            f"latency:    {LATENCY_PROBES} serial probes / matrix, p99 < {ns_ms(LATENCY_BUDGET_NS)}",
            "",
            f"{'MACHINE':<16} {'WORKSPACE':<20} {'SENT':>6} {'RECV':>6} {'FOREIGN':>8} "
            f"{'P50':>10} {'P99':>10} {'MAX':>10} {'RESULT':>8}",
            "-" * 96,
        ]
        for row in self.rows:
            lines.append(
                f"{row.machine:<16} {row.workspace:<20} {row.sent:>6} {row.recv:>6} {row.foreign:>8} "
                f"{ns_ms(row.median_ns):>10} {ns_ms(row.p99_ns):>10} {ns_ms(row.max_ns):>10} {row.result:>8}"
            )
        lines.extend(
            [
                "-" * 96,
                "",
                "ASSERTIONS",
                f"  [{'PASS' if self.honest_recv == self.honest_sent else 'FAIL'}] "
                f"100% of honest messages reached the correct orchestrator "
                f"({self.honest_recv}/{self.honest_sent})",
                f"  [{'PASS' if self.leakage == 0 else 'FAIL'}] "
                f"0% cross-talk ({self.leakage} foreign envelopes across {len(self.rows)} inboxes)",
                f"  [{'PASS' if self.attacks_accepted == 0 and self.attacks_rejected == self.attacks_sent else 'FAIL'}] "
                f"{self.attacks_rejected}/{self.attacks_sent} cross-matrix injections rejected "
                f"({self.attacks_accepted} leaked)",
                f"  [{'PASS' if self.hook_recv == self.hook_sent else 'FAIL'}] "
                f"shell hook delivered {self.hook_recv}/{self.hook_sent} pane-local alerts",
                f"  [{'PASS' if all(r.latency_ok for r in self.rows) else 'FAIL'}] "
                f"p99 push delivery latency under 5ms "
                f"(worst p99 {ns_ms(max(r.p99_ns for r in self.rows))})",
                f"  [{'PASS' if self.path_contract_ok else 'FAIL'}] "
                f"production path is /tmp/herdr_${{HERDR_MACHINE_ID}}_${{HERDR_WORKSPACE_ID}}.sock",
                f"  [{'PASS' if self.bind_exclusive_ok else 'FAIL'}] "
                f"two orchestrators cannot bind the same scoped socket",
                "",
            ]
        )
        if self.errors:
            lines.append("ERRORS")
            lines.extend(f"  - {e}" for e in self.errors)
            lines.append("")
        all_pass = True
        try:
            self.assert_all()
        except AssertionError:
            all_pass = False
        lines.append("ALL ASSERTIONS PASSED" if all_pass else "ASSERTIONS FAILED")
        lines.append("=" * 96)
        lines.append("")
        return "\n".join(lines)


class ProcessDaemon:
    """Orchestrator running in its own process — the production topology."""

    def __init__(self, machine_id: str, workspace_id: str, socket_dir: str, ctx: mp.context.BaseContext) -> None:
        self.machine_id = machine_id
        self.workspace_id = workspace_id
        self.socket_dir = socket_dir
        self.socket_path = socket_path(machine_id, workspace_id, socket_dir)
        self._inbox_q = ctx.Queue()
        self._reject_q = ctx.Queue()
        self._ready = ctx.Event()
        self._stop = ctx.Event()
        self._accepted: list[dict[str, Any]] = []
        self._rejected: list[dict[str, Any]] = []
        self._proc = ctx.Process(
            target=process_main,
            args=(
                machine_id,
                workspace_id,
                socket_dir,
                self._inbox_q,
                self._reject_q,
                self._ready,
                self._stop,
            ),
            name=f"herdr-orch-{machine_id}-{workspace_id}",
            daemon=True,
        )
        self._proc.start()
        if not self._ready.wait(5):
            raise RuntimeError(f"orchestrator process failed to bind {self.socket_path}")

    @property
    def key(self) -> bytes:
        return read_key(key_path(self.socket_path))

    def _pump(self) -> None:
        while True:
            try:
                self._accepted.append(self._inbox_q.get_nowait())
            except queue.Empty:
                break
        while True:
            try:
                self._rejected.append(self._reject_q.get_nowait())
            except queue.Empty:
                break

    def snapshot(self) -> list[dict[str, Any]]:
        self._pump()
        return list(self._accepted)

    def reject_snapshot(self) -> list[dict[str, Any]]:
        self._pump()
        return list(self._rejected)

    def stop(self) -> None:
        self._stop.set()
        self._proc.join(5)
        if self._proc.is_alive():
            self._proc.terminate()
            self._proc.join(2)


def _env_for(machine: str, workspace: str, pane: str, socket_dir: str) -> dict[str, str]:
    env = os.environ.copy()
    env["HERDR_MACHINE_ID"] = machine
    env["HERDR_WORKSPACE_ID"] = workspace
    env["HERDR_PANE_ID"] = pane
    env["HERDR_SESSION_ID"] = f"{workspace}:{pane}"
    env["HERDR_IPC_SOCKET_DIR"] = socket_dir
    return env


def _worker_job(args: tuple[Any, ...]) -> list[str]:
    machine, workspace, pane, socket_dir, start, count, barrier = args
    env = _env_for(machine, workspace, pane, socket_dir)
    barrier.wait(timeout=10)
    nonces: list[str] = []
    last_error: Exception | None = None
    for seq in range(start, start + count):
        status = STATUSES[seq % len(STATUSES)]
        try:
            result = push(
                status,
                {
                    "phase": "measure",
                    "seq": seq,
                    "worker": pane,
                    "expected_machine": machine,
                    "expected_workspace": workspace,
                },
                environ=env,
                socket_dir=socket_dir,
                wait_ack=False,
                timeout_s=0.5,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            break
        if not result.ok or not result.nonce:
            last_error = PushError(f"honest push failed for {machine}/{workspace}/{pane} seq={seq}")
            break
        nonces.append(result.nonce)
    if last_error is not None and not nonces:
        raise last_error
    return nonces


def run_matrix() -> MatrixReport:
    errors: list[str] = []
    # macOS sun_path is 104 bytes; keep the test dir under /tmp, not $TMPDIR.
    socket_dir = f"/tmp/hipc-{os.getpid()}-{secrets.token_hex(3)}"
    os.makedirs(socket_dir, mode=0o700)
    daemons: dict[str, ProcessDaemon] = {}
    sent_nonces: dict[str, set[str]] = defaultdict(set)
    ctx = mp.get_context("spawn")

    path_contract_ok = True
    for machine, workspace in matrices():
        expected = Path("/tmp") / f"herdr_{machine}_{workspace}.sock"
        if production_socket_path(machine, workspace) != expected:
            path_contract_ok = False
            errors.append(f"path contract: {production_socket_path(machine, workspace)} != {expected}")
        if socket_name(machine, workspace) != f"herdr_{machine}_{workspace}.sock":
            path_contract_ok = False

    try:
        validate_id("evil/../tmp", "HERDR_MACHINE_ID")
        path_contract_ok = False
        errors.append("validate_id accepted a path-like identifier")
    except RoutingError:
        pass

    for machine, workspace in matrices():
        daemon = ProcessDaemon(machine, workspace, socket_dir, ctx)
        daemons[key_of(machine, workspace)] = daemon
        mode = stat.S_IMODE(os.stat(daemon.socket_path).st_mode)
        if mode != 0o600:
            errors.append(f"{daemon.socket_path} mode is {oct(mode)}, expected 0o600")

    bind_exclusive_ok = True
    first = daemons[key_of(*matrices()[0])]
    clash = OrchestratorDaemon(first.machine_id, first.workspace_id, socket_dir=socket_dir)
    try:
        clash.start(timeout=1.0)
        bind_exclusive_ok = False
        errors.append("second orchestrator bound an occupied socket")
        clash.stop()
    except (RuntimeError, OSError):
        bind_exclusive_ok = True

    try:
        for machine, workspace in matrices():
            env = _env_for(machine, workspace, "warmup_pane", socket_dir)
            for i in range(WARMUP_PER_MATRIX):
                push(
                    STATUSES[i % 3],
                    {"phase": "warmup", "seq": i},
                    environ=env,
                    socket_dir=socket_dir,
                    timeout_s=0.5,
                )
        if not wait_until(
            lambda: all(len(d.snapshot()) >= WARMUP_PER_MATRIX for d in daemons.values()),
            timeout=5,
        ):
            errors.append("warmup did not drain")

        for machine, workspace in matrices():
            env = _env_for(machine, workspace, "latency_pane", socket_dir)
            for i in range(LATENCY_PROBES):
                push(
                    STATUSES[i % 3],
                    {"phase": "latency", "seq": i},
                    environ=env,
                    socket_dir=socket_dir,
                    wait_ack=True,
                    timeout_s=0.5,
                )
        if not wait_until(
            lambda: all(
                sum(1 for m in d.snapshot() if (m.get("payload") or {}).get("phase") == "latency")
                >= LATENCY_PROBES
                for d in daemons.values()
            ),
            timeout=5,
        ):
            errors.append("latency probe did not drain")

        barrier = threading.Barrier(WORKERS_PER_MATRIX * len(matrices()))
        jobs = []
        for machine, workspace in matrices():
            for w in range(WORKERS_PER_MATRIX):
                pane = f"pane_{w:02d}"
                jobs.append((machine, workspace, pane, socket_dir, w * MESSAGES_PER_WORKER, MESSAGES_PER_WORKER, barrier))

        with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
            futures = [pool.submit(_worker_job, job) for job in jobs]
            for fut, job in zip(futures, jobs):
                machine, workspace = job[0], job[1]
                try:
                    nonces = fut.result(timeout=30)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"worker {machine}/{workspace}/{job[2]}: {exc}")
                    nonces = []
                if len(nonces) != MESSAGES_PER_WORKER:
                    errors.append(
                        f"worker {machine}/{workspace}/{job[2]} delivered {len(nonces)}/{MESSAGES_PER_WORKER}"
                    )
                sent_nonces[key_of(machine, workspace)].update(nonces)

        expected_per = WORKERS_PER_MATRIX * MESSAGES_PER_WORKER
        drained = wait_until(
            lambda: all(
                sum(1 for m in d.snapshot() if (m.get("payload") or {}).get("phase") == "measure")
                >= expected_per
                for d in daemons.values()
            ),
            timeout=8,
        )
        if not drained:
            errors.append("measured burst did not fully drain into orchestrator inboxes")
        for machine, workspace in matrices():
            for record in daemons[key_of(machine, workspace)].snapshot():
                sender = record.get("sender") or {}
                if (
                    sender.get("kind") != "agent"
                    or sender.get("id") != record.get("pane_id")
                    or sender.get("session_id") != record.get("session_id")
                    or sender.get("pane_id") != record.get("pane_id")
                ):
                    errors.append(f"sender identity mismatch in {machine}/{workspace}: {record}")

        attacks_sent = 0
        attacks_rejected = 0
        attacks_accepted = 0
        for src_m, src_w in matrices():
            src = daemons[key_of(src_m, src_w)]
            src_env = _env_for(src_m, src_w, "attacker", socket_dir)
            src_key = src.key
            assert src_key is not None
            for dst_m, dst_w in matrices():
                if (src_m, src_w) == (dst_m, dst_w):
                    continue
                dst = daemons[key_of(dst_m, dst_w)]
                honest_wrong_socket = build_envelope(
                    machine_id=src_m,
                    workspace_id=src_w,
                    pane_id="attacker",
                    session_id="attacker-session",
                    sender={
                        "kind": "agent",
                        "id": "attacker",
                        "session_id": "attacker-session",
                        "pane_id": "attacker",
                    },
                    status="blocked",
                    payload={"phase": "attack", "kind": "wrong_socket"},
                )
                forged_identity = build_envelope(
                    machine_id=dst_m,
                    workspace_id=dst_w,
                    pane_id="attacker",
                    session_id="attacker-session",
                    sender={
                        "kind": "agent",
                        "id": "attacker",
                        "session_id": "attacker-session",
                        "pane_id": "attacker",
                    },
                    status="done",
                    payload={"phase": "attack", "kind": "forged_identity"},
                )
                cases = [
                    push_forged_for_tests(
                        target_machine=dst_m,
                        target_workspace=dst_w,
                        envelope=honest_wrong_socket,
                        socket_dir=socket_dir,
                        hmac_key=src_key,
                    ),
                    push_forged_for_tests(
                        target_machine=dst_m,
                        target_workspace=dst_w,
                        envelope=forged_identity,
                        socket_dir=socket_dir,
                        hmac_key=src_key,
                    ),
                    push_forged_for_tests(
                        target_machine=dst_m,
                        target_workspace=dst_w,
                        envelope=forged_identity,
                        socket_dir=socket_dir,
                        hmac_key=None,
                    ),
                ]
                # Also: live client with src env cannot address dst even if we
                # point HERDR_IPC_SOCKET_DIR at the same dir — identity is src.
                try:
                    live = push(
                        "working",
                        {"phase": "attack", "kind": "env_cannot_retarget"},
                        environ=src_env,
                        socket_dir=socket_dir,
                        wait_ack=True,
                        timeout_s=0.5,
                    )
                    if live.socket != str(socket_path(src_m, src_w, socket_dir)):
                        errors.append("client addressed a socket that did not match its env identity")
                except PushError:
                    pass
                for ack in cases:
                    attacks_sent += 1
                    if ack.get("ok"):
                        attacks_accepted += 1
                    else:
                        attacks_rejected += 1
                _ = dst  # isolation is proven by ack.ok == False plus inbox scan

        hook_sent = 0
        hook_recv = 0
        if HOOK.is_file():
            os.chmod(HOOK, 0o755)
            for machine, workspace in matrices():
                env = _env_for(machine, workspace, "hook_pane", socket_dir)
                env["HERDR_STATUS"] = "done"
                env["HERDR_IPC_WAIT_ACK"] = "1"
                env["HERDR_IPC_TIMEOUT_MS"] = "500"
                before = {
                    rec["nonce"]
                    for rec in daemons[key_of(machine, workspace)].snapshot()
                    if rec.get("pane_id") == "hook_pane"
                }
                proc = subprocess.run(
                    [str(HOOK), "done", '{"phase":"hook","via":"herdr-push-hook.sh"}'],
                    env=env,
                    cwd=str(ROOT),
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                hook_sent += 1
                if proc.returncode != 0:
                    errors.append(f"hook failed {machine}/{workspace}: {proc.stderr.strip()}")
                    continue
                landed = wait_until(
                    lambda m=machine, w=workspace, b=before: len(
                        {
                            rec["nonce"]
                            for rec in daemons[key_of(m, w)].snapshot()
                            if rec.get("pane_id") == "hook_pane"
                        }
                        - b
                    )
                    >= 1,
                    timeout=2,
                )
                if landed:
                    hook_recv += 1
                else:
                    errors.append(f"hook message never appeared in {machine}/{workspace}")
        else:
            errors.append(f"missing {HOOK}")

        rows: list[MatrixRow] = []
        honest_sent = 0
        honest_recv = 0
        leakage = 0
        missing: list[str] = []
        extra: list[str] = []
        for machine, workspace in matrices():
            k = key_of(machine, workspace)
            daemon = daemons[k]
            inbox = [
                rec
                for rec in daemon.snapshot()
                if (rec.get("payload") or {}).get("phase") == "measure"
            ]
            foreign = [
                rec
                for rec in daemon.snapshot()
                if rec.get("machine_id") != machine or rec.get("workspace_id") != workspace
            ]
            # A message that belongs to another matrix but landed here.
            foreign += [
                rec
                for rec in inbox
                if (rec.get("payload") or {}).get("expected_machine") not in (None, machine)
                or (rec.get("payload") or {}).get("expected_workspace") not in (None, workspace)
            ]
            # Unique foreign by nonce
            foreign_nonces = {rec.get("nonce") for rec in foreign if rec.get("nonce")}
            got = {rec["nonce"] for rec in inbox}
            expected = sent_nonces[k]
            missing.extend(sorted(expected - got)[:32])
            extra.extend(sorted(got - expected)[:32])
            latency_recs = [
                rec
                for rec in daemon.snapshot()
                if (rec.get("payload") or {}).get("phase") == "latency" and "latency_ns" in rec
            ]
            latencies = [int(rec["latency_ns"]) for rec in latency_recs]
            if len(latencies) < LATENCY_PROBES:
                errors.append(f"latency samples for {k}: {len(latencies)}/{LATENCY_PROBES}")
            if not latencies:
                latencies = [LATENCY_BUDGET_NS + 1]
            row = MatrixRow(
                machine=machine,
                workspace=workspace,
                sent=len(expected),
                recv=len(got & expected),
                foreign=len(foreign_nonces),
                rejected=len(daemon.reject_snapshot()),
                min_ns=min(latencies),
                median_ns=percentile(latencies, 50),
                p99_ns=percentile(latencies, 99),
                max_ns=max(latencies),
            )
            rows.append(row)
            honest_sent += row.sent
            honest_recv += row.recv
            leakage += row.foreign

        return MatrixReport(
            socket_dir=socket_dir,
            rows=rows,
            honest_sent=honest_sent,
            honest_recv=honest_recv,
            leakage=leakage,
            attacks_sent=attacks_sent,
            attacks_rejected=attacks_rejected,
            attacks_accepted=attacks_accepted,
            hook_sent=hook_sent,
            hook_recv=hook_recv,
            missing_nonces=missing,
            extra_nonces=extra,
            path_contract_ok=path_contract_ok,
            bind_exclusive_ok=bind_exclusive_ok,
            errors=errors,
        )
    finally:
        for daemon in daemons.values():
            try:
                daemon.stop()
            except Exception:
                traceback.print_exc()
        shutil.rmtree(socket_dir, ignore_errors=True)


class IsolationMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = run_matrix()
        sys.stdout.write(cls.report.render())
        sys.stdout.flush()

    def test_path_contract(self) -> None:
        self.assertTrue(self.report.path_contract_ok)

    def test_exclusive_bind(self) -> None:
        self.assertTrue(self.report.bind_exclusive_ok)

    def test_full_delivery(self) -> None:
        self.assertEqual(self.report.honest_recv, self.report.honest_sent)
        self.assertGreater(self.report.honest_sent, 0)

    def test_zero_leakage(self) -> None:
        self.assertEqual(self.report.leakage, 0)
        self.assertEqual(self.report.attacks_accepted, 0)
        self.assertEqual(self.report.attacks_rejected, self.report.attacks_sent)
        self.assertGreater(self.report.attacks_sent, 0)

    def test_latency_under_5ms(self) -> None:
        for row in self.report.rows:
            self.assertLess(
                row.p99_ns,
                LATENCY_BUDGET_NS,
                f"{row.machine}/{row.workspace} p99 {ns_ms(row.p99_ns)}",
            )

    def test_shell_hook(self) -> None:
        self.assertEqual(self.report.hook_recv, self.report.hook_sent)
        self.assertEqual(self.report.hook_sent, len(matrices()))

    def test_all_assertions(self) -> None:
        self.report.assert_all()


def main() -> int:
    report = run_matrix()
    sys.stdout.write(report.render())
    sys.stdout.flush()
    try:
        report.assert_all()
    except AssertionError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
