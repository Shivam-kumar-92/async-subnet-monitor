#!/usr/bin/env python3
"""Async Subnet Health & TCP Port Latency Monitor.

Performs bounded-concurrency asyncio CIDR subnet discovery, TCP port latency
measurement, and exports a structured JSON health report for NOC/SRE pipelines.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import ipaddress
import json
from pathlib import Path
import sys
import time
from typing import Sequence


@dataclass(frozen=True)
class PortProbe:
    port: int
    status: str  # "OPEN" | "CLOSED" | "TIMEOUT"
    latency_ms: float | None = None
    error: str | None = None


@dataclass(frozen=True)
class HostReport:
    ip: str
    reachable: bool
    open_ports: list[int]
    min_latency_ms: float | None
    avg_latency_ms: float | None
    ports: list[PortProbe]


async def probe_tcp_port(host: str, port: int, timeout: float) -> PortProbe:
    """Measure TCP 3-way handshake latency on host:port using asyncio."""
    start = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        latency_ms = round((time.perf_counter() - start) * 1000.0, 2)
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        _ = reader
        return PortProbe(port=port, status="OPEN", latency_ms=latency_ms)
    except asyncio.TimeoutError:
        return PortProbe(port=port, status="TIMEOUT", error=f"Timed out after {timeout}s")
    except OSError as exc:
        return PortProbe(port=port, status="CLOSED", error=exc.strerror or str(exc))


async def check_host(
    ip: str,
    ports: Sequence[int],
    timeout: float,
    semaphore: asyncio.Semaphore,
) -> HostReport:
    """Probe all configured TCP ports on a single host under concurrency control."""
    async with semaphore:
        probes = await asyncio.gather(*(probe_tcp_port(ip, p, timeout) for p in ports))

    open_probes = [p for p in probes if p.status == "OPEN"]
    latencies = [p.latency_ms for p in open_probes if p.latency_ms is not None]
    min_lat = round(min(latencies), 2) if latencies else None
    avg_lat = round(sum(latencies) / len(latencies), 2) if latencies else None

    return HostReport(
        ip=ip,
        reachable=bool(open_probes),
        open_ports=[p.port for p in open_probes],
        min_latency_ms=min_lat,
        avg_latency_ms=avg_lat,
        ports=list(probes),
    )


def expand_targets(cidr: str) -> list[str]:
    """Expand a CIDR block (e.g., 192.168.1.0/24 or 127.0.0.1/32) into host IPs."""
    network = ipaddress.ip_network(cidr, strict=False)
    if network.num_addresses == 1:
        return [str(network.network_address)]
    return [str(ip) for ip in network.hosts()]


async def scan_subnet(
    cidr: str,
    ports: Sequence[int],
    timeout: float = 1.5,
    concurrency: int = 64,
) -> dict:
    """Scan all hosts in a CIDR subnet concurrently and build a structured JSON report."""
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    t0 = time.perf_counter()

    targets = expand_targets(cidr)
    sem = asyncio.Semaphore(max(1, concurrency))
    host_reports = await asyncio.gather(
        *(check_host(ip, ports, timeout, sem) for ip in targets)
    )

    elapsed_sec = round(time.perf_counter() - t0, 3)
    reachable_hosts = [h for h in host_reports if h.reachable]

    return {
        "schema_version": "1.0",
        "timestamp_utc": started_at,
        "subnet": cidr,
        "ports_checked": list(ports),
        "timeout_seconds": timeout,
        "scan_duration_seconds": elapsed_sec,
        "summary": {
            "total_hosts": len(host_reports),
            "reachable_hosts": len(reachable_hosts),
            "unreachable_hosts": len(host_reports) - len(reachable_hosts),
            "availability_pct": (
                round((len(reachable_hosts) / len(host_reports)) * 100.0, 2)
                if host_reports
                else 0.0
            ),
        },
        "hosts": [asdict(h) for h in host_reports],
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Async CIDR Subnet Health & TCP Port Latency Monitor"
    )
    parser.add_argument(
        "--subnet",
        required=True,
        help="Target IPv4/IPv6 CIDR block (e.g., 192.168.1.0/28 or 127.0.0.1/32)",
    )
    parser.add_argument(
        "--ports",
        default="22,53,80,443",
        help="Comma-separated TCP ports to probe (default: 22,53,80,443)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.5,
        help="Per-port TCP connect timeout in seconds (default: 1.5)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=64,
        help="Max concurrent host scans via asyncio.Semaphore (default: 64)",
    )
    parser.add_argument(
        "--output",
        default="subnet_report.json",
        help="Output JSON report path (default: subnet_report.json)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ports = [int(p.strip()) for p in args.ports.split(",") if p.strip()]
    if not ports or any(not (1 <= p <= 65535) for p in ports):
        print("ERROR: --ports must contain valid TCP port numbers (1-65535).", file=sys.stderr)
        return 2

    report = asyncio.run(
        scan_subnet(
            cidr=args.subnet,
            ports=ports,
            timeout=args.timeout,
            concurrency=args.concurrency,
        )
    )

    out_path = Path(args.output)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    s = report["summary"]
    print(
        f"Scan complete for {report['subnet']} in {report['scan_duration_seconds']}s | "
        f"Reachable: {s['reachable_hosts']}/{s['total_hosts']} ({s['availability_pct']}%) | "
        f"Report saved -> {out_path.resolve()}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
