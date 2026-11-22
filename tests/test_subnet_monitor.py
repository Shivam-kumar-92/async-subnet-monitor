"""Automated unit & integration tests for Async Subnet Health Monitor."""

from __future__ import annotations

import asyncio
import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path

import subnet_health_check as shc


class TestAsyncSubnetMonitor(unittest.TestCase):
    def test_expand_targets_single_and_subnet(self) -> None:
        self.assertEqual(shc.expand_targets("127.0.0.1/32"), ["127.0.0.1"])
        hosts = shc.expand_targets("10.20.10.0/30")
        self.assertEqual(hosts, ["10.20.10.1", "10.20.10.2"])

    def test_scan_open_and_closed_local_ports(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        open_port = srv.getsockname()[1]

        def accept_once() -> None:
            try:
                conn, _ = srv.accept()
                conn.close()
            except OSError:
                pass

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unused:
            unused.bind(("127.0.0.1", 0))
            closed_port = unused.getsockname()[1]

        t = threading.Thread(target=accept_once, daemon=True)
        t.start()
        try:
            report = asyncio.run(
                shc.scan_subnet(
                    cidr="127.0.0.1/32",
                    ports=[open_port, closed_port],
                    timeout=1.0,
                    concurrency=4,
                )
            )
        finally:
            t.join(timeout=2.0)
            srv.close()

        self.assertEqual(report["summary"]["total_hosts"], 1)
        self.assertEqual(report["summary"]["reachable_hosts"], 1)
        self.assertEqual(report["summary"]["availability_pct"], 100.0)
        host = report["hosts"][0]
        self.assertTrue(host["reachable"])
        self.assertEqual(host["open_ports"], [open_port])
        self.assertIsNotNone(host["min_latency_ms"])

    def test_cli_writes_json_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_file = Path(tmp) / "out.json"
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unused:
                unused.bind(("127.0.0.1", 0))
                closed_port = unused.getsockname()[1]

            rc = shc.main(
                [
                    "--subnet",
                    "127.0.0.1/32",
                    "--ports",
                    str(closed_port),
                    "--timeout",
                    "0.5",
                    "--output",
                    str(out_file),
                ]
            )
            self.assertEqual(rc, 0)
            data = json.loads(out_file.read_text(encoding="utf-8"))
            self.assertEqual(data["schema_version"], "1.0")
            self.assertEqual(data["summary"]["total_hosts"], 1)


if __name__ == "__main__":
    unittest.main()
