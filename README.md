# Async Subnet Health & Port Latency Monitor

[![CI](https://github.com/Shivam-kumar-92/async-subnet-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/Shivam-kumar-92/async-subnet-monitor/actions/workflows/ci.yml)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![Dependencies](https://img.shields.io/badge/dependencies-zero%20(stdlib)-brightgreen)

> High-concurrency Python `asyncio` network automation tool for automated CIDR subnet health sweeps, TCP port latency monitoring, and structured JSON telemetry export.

---

## Problem Statement
Sequential `ping` and `nc` loops take minutes to audit even a single `/24` subnet and produce unstructured terminal text that cannot feed downstream alerting or AI triage pipelines. This tool uses bounded `asyncio` concurrency to sweep entire CIDR blocks in seconds and output machine-readable JSON health reports.

---

## Architecture

```text
  CIDR Input (e.g., 10.20.10.0/24)
               |
               v
  +-----------------------------+
  |  ipaddress CIDR Expander    |
  +--------------+--------------+
                 |
                 v
  +-----------------------------+
  |  asyncio.Semaphore(64) Pool |  <-- Prevents FD exhaustion & SYN storms
  +--------------+--------------+
                 |
      +----------+----------+
      v                     v
[ Host 1: 22/53/80/443 ]  [ Host N: 22/53/80/443 ]   (asyncio.open_connection)
      |                     |
      +----------+----------+
                 v
  +-----------------------------+
  |  JSON Telemetry Exporter    |  --> subnet_report.json (SLA + Latency ms)
  +-----------------------------+
```

---

## Key Engineering Decisions
- **Bounded Concurrency (`asyncio.Semaphore`):** Caps in-flight sockets so scanning large subnets never trips OS `ulimit` file-descriptor caps or triggers IDS SYN-flood heuristics.
- **Zero Runtime Dependencies:** Uses Python's standard library (`asyncio`, `ipaddress`, `json`) so it runs reliably on locked-down Linux jump hosts, EC2 instances, and containers without privileged raw-socket (`CAP_NET_RAW`) requirements.
- **Structured JSON Contract:** Outputs a versioned schema (`schema_version`, `timestamp_utc`, `summary`, per-host `min_latency_ms`/`avg_latency_ms`, and per-port `OPEN`/`CLOSED`/`TIMEOUT` status) ready for SIEM, dashboards, or LLM incident-summarization pipelines.

---

## Quickstart

```bash
# Scan a local subnet on SSH, DNS, HTTP, and HTTPS ports
python subnet_health_check.py --subnet 192.168.1.0/28 --ports 22,53,80,443 --output report.json

# Quick single-host sanity check
python subnet_health_check.py --subnet 1.1.1.1/32 --ports 53,80,443 --timeout 2.0 --output cf_report.json
```
