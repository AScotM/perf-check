#!/usr/bin/env python3

import argparse
import csv
import json
import math
import os
import re
import signal
import socket
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


FIELDS = [
    "timestamp", "host", "elapsed_s", "cpu_usage", "load1", "load5", "load15",
    "mem_total", "mem_available", "mem_used_pct", "swap_total", "swap_free",
    "swap_used_pct", "swapin_rate", "swapout_rate", "read_iops", "write_iops",
    "read_Bps", "write_Bps", "rx_rate", "tx_rate", "cpu_pressure_some",
    "memory_pressure_some", "io_pressure_some", "health", "alerts",
]


@dataclass
class Config:
    proc_root: Path = Path("/proc")
    sys_root: Path = Path("/sys")
    include_loopback: bool = False
    include_virtual_disks: bool = False
    include_partitions: bool = False


def read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeError):
        return None


def number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def percentage(part: float, whole: float) -> float:
    if whole <= 0:
        return 0.0
    return max(0.0, min(100.0, 100.0 * part / whole))


def counter_rate(current: int, previous: int, elapsed: float) -> float:
    if elapsed <= 0 or current < previous:
        return 0.0
    return (current - previous) / elapsed


def parse_load(text: Optional[str]) -> Dict[str, float]:
    parts = (text or "").split()
    return {key: number(parts[i]) if len(parts) > i else 0.0
            for i, key in enumerate(("load1", "load5", "load15"))}


def parse_meminfo(text: Optional[str]) -> Dict[str, int]:
    values = {}
    for line in (text or "").splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        parts = raw.split()
        if not parts:
            continue
        value = integer(parts[0])
        values[key] = value * 1024 if len(parts) > 1 and parts[1] == "kB" else value
    return {
        "mem_total": values.get("MemTotal", 0),
        "mem_available": values.get("MemAvailable", values.get("MemFree", 0)),
        "mem_free": values.get("MemFree", 0),
        "mem_cached": values.get("Cached", 0),
        "mem_buffers": values.get("Buffers", 0),
        "swap_total": values.get("SwapTotal", 0),
        "swap_free": values.get("SwapFree", 0),
        "dirty_bytes": values.get("Dirty", 0),
    }


def parse_vmstat(text: Optional[str]) -> Dict[str, int]:
    values = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) == 2:
            values[parts[0]] = integer(parts[1])
    return {"pswpin": values.get("pswpin", 0), "pswpout": values.get("pswpout", 0),
            "pgfault": values.get("pgfault", 0), "pgmajfault": values.get("pgmajfault", 0)}


def parse_cpu(text: Optional[str]) -> Dict[str, Dict[str, int]]:
    result = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if not parts or not re.fullmatch(r"cpu\d*", parts[0]):
            continue
        try:
            values = [int(v) for v in parts[1:]]
        except ValueError:
            continue
        if len(values) < 4:
            continue
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        guest = (values[8] if len(values) > 8 else 0) + (values[9] if len(values) > 9 else 0)
        total = sum(values) - guest
        result[parts[0]] = {"idle": idle, "total": total,
                            "iowait": values[4] if len(values) > 4 else 0,
                            "steal": values[7] if len(values) > 7 else 0}
    return result


def cpu_percent(current: Dict[str, int], previous: Optional[Dict[str, int]]) -> float:
    if previous is None:
        return 0.0
    delta = current["total"] - previous["total"]
    if delta <= 0:
        return 0.0
    return percentage(delta - (current["idle"] - previous["idle"]), delta)


def parse_diskstats(text: Optional[str], config: Config) -> Dict[str, Dict[str, int]]:
    disks = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        device = parts[2]
        if not re.fullmatch(r"[A-Za-z0-9_.!+-]+", device):
            continue
        if not config.include_virtual_disks and device.startswith(("loop", "ram", "zram", "dm-", "md")):
            continue
        if not config.include_partitions:
            if (config.sys_root / "block" / device).exists():
                pass
            elif (config.sys_root / "class" / "block" / device / "partition").exists():
                continue
            elif re.fullmatch(r"(?:sd[a-z]+\d+|vd[a-z]+\d+|xvd[a-z]+\d+|nvme\d+n\d+p\d+|mmcblk\d+p\d+)", device):
                continue
        try:
            values = [int(v) for v in parts[3:]]
        except ValueError:
            continue
        disks[device] = {
            "read_ios": values[0], "read_sectors": values[2],
            "write_ios": values[4], "write_sectors": values[6],
            "io_ms": values[9] if len(values) > 9 else 0,
            "weighted_io_ms": values[10] if len(values) > 10 else 0,
        }
    return disks


def parse_netdev(text: Optional[str], include_loopback: bool = False) -> Dict[str, Dict[str, int]]:
    interfaces = {}
    for line in (text or "").splitlines():
        if ":" not in line:
            continue
        name, raw = line.rsplit(":", 1)
        name = name.strip()
        if not name or (name == "lo" and not include_loopback):
            continue
        parts = raw.split()
        if len(parts) < 16:
            continue
        try:
            values = [int(x) for x in parts[:16]]
        except ValueError:
            continue
        interfaces[name] = {
            "rx_bytes": values[0], "rx_packets": values[1],
            "rx_errors": values[2], "rx_dropped": values[3],
            "tx_bytes": values[8], "tx_packets": values[9],
            "tx_errors": values[10], "tx_dropped": values[11],
        }
    return interfaces


def parse_pressure(text: Optional[str]) -> Dict[str, Dict[str, float]]:
    result = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if not parts or parts[0] not in ("some", "full"):
            continue
        metrics = {}
        for part in parts[1:]:
            if "=" in part:
                key, value = part.split("=", 1)
                metrics[key] = number(value)
        result[parts[0]] = metrics
    return result


def snapshot(config: Config) -> Dict[str, Any]:
    root = config.proc_root
    return {
        "timestamp": time.time(),
        "monotonic": time.monotonic(),
        "host": socket.gethostname(),
        "load": parse_load(read_text(root / "loadavg")),
        "memory": parse_meminfo(read_text(root / "meminfo")),
        "vmstat": parse_vmstat(read_text(root / "vmstat")),
        "cpu": parse_cpu(read_text(root / "stat")),
        "disks": parse_diskstats(read_text(root / "diskstats"), config),
        "interfaces": parse_netdev(read_text(root / "net" / "dev"), config.include_loopback),
        "pressure": {kind: parse_pressure(read_text(root / "pressure" / kind))
                     for kind in ("cpu", "memory", "io")},
    }


def device_rates(current: Dict[str, Dict[str, int]], previous: Dict[str, Dict[str, int]],
                 elapsed: float, keys: tuple) -> Dict[str, Dict[str, float]]:
    result = {}
    for name, counters in current.items():
        old = previous.get(name, {})
        result[name] = {key + "_rate": counter_rate(value, old.get(key, value), elapsed)
                        for key, value in counters.items() if key in keys}
    return result


def total_rates(items: Dict[str, Dict[str, float]], key: str) -> float:
    return sum(entry.get(key + "_rate", 0.0) for entry in items.values())


def build_report(current: Dict[str, Any], previous: Optional[Dict[str, Any]],
                 warning: float = 80.0, critical: float = 95.0) -> Dict[str, Any]:
    elapsed = max(0.0, current["monotonic"] - previous["monotonic"]) if previous else 0.0
    cpu = current["cpu"]
    previous_cpu = previous["cpu"] if previous else {}
    per_core = {key: cpu_percent(value, previous_cpu.get(key))
                for key, value in cpu.items() if key != "cpu"}
    cpu_usage = cpu_percent(cpu["cpu"], previous_cpu.get("cpu")) if "cpu" in cpu else 0.0
    memory = current["memory"]
    mem_used_pct = percentage(memory["mem_total"] - memory["mem_available"], memory["mem_total"])
    swap_used_pct = percentage(memory["swap_total"] - memory["swap_free"], memory["swap_total"])
    previous_disks = previous["disks"] if previous else {}
    previous_net = previous["interfaces"] if previous else {}
    disks = device_rates(current["disks"], previous_disks, elapsed,
                         ("read_ios", "write_ios", "read_sectors", "write_sectors", "io_ms"))
    for item in disks.values():
        item["read_Bps"] = item.pop("read_sectors_rate", 0.0) * 512
        item["write_Bps"] = item.pop("write_sectors_rate", 0.0) * 512
        item["busy_pct"] = min(100.0, item.pop("io_ms_rate", 0.0) / 10.0)
    interfaces = device_rates(current["interfaces"], previous_net, elapsed,
                              ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets",
                               "rx_errors", "tx_errors", "rx_dropped", "tx_dropped"))
    vm = current["vmstat"]
    old_vm = previous["vmstat"] if previous else vm
    swapin_rate = counter_rate(vm["pswpin"], old_vm["pswpin"], elapsed)
    swapout_rate = counter_rate(vm["pswpout"], old_vm["pswpout"], elapsed)
    pressure = current["pressure"]
    cpu_psi = pressure.get("cpu", {}).get("some", {}).get("avg10", 0.0)
    memory_psi = pressure.get("memory", {}).get("some", {}).get("avg10", 0.0)
    io_psi = pressure.get("io", {}).get("some", {}).get("avg10", 0.0)
    alerts = []
    def assess(name: str, value: float) -> None:
        if value >= critical:
            alerts.append({"level": "critical", "metric": name, "value": round(value, 2)})
        elif value >= warning:
            alerts.append({"level": "warning", "metric": name, "value": round(value, 2)})
    assess("cpu_usage", cpu_usage)
    assess("mem_used_pct", mem_used_pct)
    if memory["swap_total"]:
        assess("swap_used_pct", swap_used_pct)
    for name, values in disks.items():
        assess("disk_busy:" + name, values["busy_pct"])
    if memory_psi >= 10:
        alerts.append({"level": "warning", "metric": "memory_psi_avg10", "value": memory_psi})
    if io_psi >= 10:
        alerts.append({"level": "warning", "metric": "io_psi_avg10", "value": io_psi})
    health = "critical" if any(x["level"] == "critical" for x in alerts) else "warning" if alerts else "ok"
    report = {
        "timestamp": round(current["timestamp"], 3), "host": current["host"],
        "elapsed_s": round(elapsed, 6), "cpu_usage": round(cpu_usage, 3),
        "per_core": per_core, **current["load"], **memory,
        "mem_used_pct": round(mem_used_pct, 3), "swap_used_pct": round(swap_used_pct, 3),
        "swapin_rate": swapin_rate, "swapout_rate": swapout_rate,
        "read_iops": total_rates(disks, "read_ios"),
        "write_iops": total_rates(disks, "write_ios"),
        "read_Bps": sum(v["read_Bps"] for v in disks.values()),
        "write_Bps": sum(v["write_Bps"] for v in disks.values()),
        "rx_rate": total_rates(interfaces, "rx_bytes"),
        "tx_rate": total_rates(interfaces, "tx_bytes"),
        "cpu_pressure_some": cpu_psi, "memory_pressure_some": memory_psi,
        "io_pressure_some": io_psi, "pressure": pressure,
        "disks": disks, "interfaces": interfaces, "health": health, "alerts": alerts,
        "baseline": previous is not None,
    }
    return report


def human_bytes(value: float, rate: bool = False) -> str:
    value = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    index = 0
    while abs(value) >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return f"{value:.2f} {units[index]}" + ("/s" if rate else "")


def framed(report: Dict[str, Any], width: int = 100, details: bool = False) -> str:
    width = max(60, width)
    lines = [
        f"PERF CHECK  {report['host']}  {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(report['timestamp']))}",
        f"HEALTH {report['health'].upper()}   SAMPLE {report['elapsed_s']:.3f}s",
        f"CPU {report['cpu_usage']:.2f}%   LOAD {report['load1']:.2f}/{report['load5']:.2f}/{report['load15']:.2f}",
        f"MEM {report['mem_used_pct']:.2f}%   AVAILABLE {human_bytes(report['mem_available'])} / {human_bytes(report['mem_total'])}",
        f"SWAP {report['swap_used_pct']:.2f}%   IN {report['swapin_rate']:.2f}/s   OUT {report['swapout_rate']:.2f}/s",
        f"DISK READ {report['read_iops']:.2f} IOPS {human_bytes(report['read_Bps'], True)}   WRITE {report['write_iops']:.2f} IOPS {human_bytes(report['write_Bps'], True)}",
        f"NETWORK RX {human_bytes(report['rx_rate'], True)}   TX {human_bytes(report['tx_rate'], True)}",
        f"PSI AVG10 CPU {report['cpu_pressure_some']:.2f}%   MEM {report['memory_pressure_some']:.2f}%   IO {report['io_pressure_some']:.2f}%",
    ]
    if details:
        for name, value in sorted(report["per_core"].items()):
            lines.append(f"CORE {name} {value:.2f}%")
        for name, values in sorted(report["disks"].items()):
            lines.append(f"DISK {name} R {values['read_ios_rate']:.2f}/s W {values['write_ios_rate']:.2f}/s BUSY {values['busy_pct']:.2f}%")
        for name, values in sorted(report["interfaces"].items()):
            lines.append(f"NET {name} RX {human_bytes(values['rx_bytes_rate'], True)} TX {human_bytes(values['tx_bytes_rate'], True)}")
    for alert in report["alerts"]:
        lines.append(f"{alert['level'].upper()} {alert['metric']}={alert['value']}")
    border = "+" + "-" * (width - 2) + "+"
    return "\n".join([border] + ["| " + line[:width - 4].ljust(width - 4) + " |" for line in lines] + [border])


def line_format(report: Dict[str, Any]) -> str:
    return (f"{int(report['timestamp'])} {report['host']} elapsed={report['elapsed_s']:.3f}s "
            f"cpu={report['cpu_usage']:.2f}% load={report['load1']:.2f}/{report['load5']:.2f}/{report['load15']:.2f} "
            f"mem={report['mem_used_pct']:.2f}% swap={report['swap_used_pct']:.2f}% "
            f"iops={report['read_iops']:.2f}/{report['write_iops']:.2f} "
            f"net={human_bytes(report['rx_rate'], True)}/{human_bytes(report['tx_rate'], True)} "
            f"health={report['health']}")


def write_csv(path: str, report: Dict[str, Any]) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        if handle.tell() == 0:
            writer.writeheader()
        row = dict(report)
        row["alerts"] = json.dumps(row["alerts"], separators=(",", ":"))
        writer.writerow(row)


def write_jsonl(path: str, report: Dict[str, Any]) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, ensure_ascii=False, allow_nan=False) + "\n")


def parser_factory() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Linux performance and pressure monitor")
    parser.add_argument("-i", "--interval", type=float, default=0.0)
    parser.add_argument("-c", "--count", type=int, default=0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--framed", action="store_true")
    parser.add_argument("--details", action="store_true")
    parser.add_argument("--width", type=int, default=100)
    parser.add_argument("--format", choices=("line", "framed", "json", "jsonl"), default="line")
    parser.add_argument("--csv")
    parser.add_argument("--jsonl-file")
    parser.add_argument("--warning", type=float, default=80.0)
    parser.add_argument("--critical", type=float, default=95.0)
    parser.add_argument("--include-loopback", action="store_true")
    parser.add_argument("--include-virtual-disks", action="store_true")
    parser.add_argument("--include-partitions", action="store_true")
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    parser.add_argument("--sys-root", type=Path, default=Path("/sys"))
    parser.add_argument("--test", action="store_true")
    return parser


class PerformanceTests(unittest.TestCase):
    def test_counter_rate(self):
        self.assertEqual(counter_rate(20, 10, 2), 5)
        self.assertEqual(counter_rate(3, 10, 2), 0)
        self.assertEqual(counter_rate(20, 10, 0), 0)

    def test_meminfo(self):
        result = parse_meminfo("MemTotal: 100 kB\nMemAvailable: 40 kB\nSwapTotal: 0 kB")
        self.assertEqual(result["mem_total"], 102400)
        self.assertEqual(result["mem_available"], 40960)

    def test_cpu(self):
        previous = {"idle": 100, "total": 200}
        current = {"idle": 150, "total": 300}
        self.assertEqual(cpu_percent(current, previous), 50)

    def test_cpu_guest_accounting(self):
        values = parse_cpu("cpu 100 0 20 500 10 0 0 0 30 0\n")
        self.assertEqual(values["cpu"]["total"], 630)

    def test_net(self):
        sample = "eth0: 100 2 0 0 0 0 0 0 200 3 0 0 0 0 0 0"
        self.assertEqual(parse_netdev(sample)["eth0"]["tx_bytes"], 200)

    def test_pressure(self):
        psi = parse_pressure("some avg10=1.25 avg60=2.0 avg300=3.0 total=40")
        self.assertEqual(psi["some"]["avg10"], 1.25)

    def test_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Config(sys_root=Path(directory))
            line = "8 0 sda 10 0 20 0 30 0 40 0 0 50 60"
            self.assertEqual(parse_diskstats(line, config)["sda"]["write_sectors"], 40)

    def test_report_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Config(proc_root=Path(directory))
            first = snapshot(config)
            report = build_report(first, None)
            self.assertFalse(report["baseline"])
            self.assertEqual(report["cpu_usage"], 0)

    def test_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "out.csv")
            write_csv(path, {"timestamp": 1, "host": "test", "alerts": []})
            with open(path, encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.reader(handle))), 2)

    def test_nonfinite(self):
        self.assertEqual(number("nan"), 0)
        self.assertEqual(number("inf"), 0)


def main() -> int:
    parser = parser_factory()
    args = parser.parse_args()
    if args.test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(PerformanceTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not math.isfinite(args.interval) or args.interval < 0:
        parser.error("--interval must be finite and nonnegative")
    if args.count < 0:
        parser.error("--count must be nonnegative")
    if not all(math.isfinite(x) for x in (args.warning, args.critical)):
        parser.error("thresholds must be finite")
    if not 0 <= args.warning < args.critical <= 100:
        parser.error("thresholds must satisfy 0 <= warning < critical <= 100")
    if args.width < 60:
        parser.error("--width must be at least 60")
    if args.interval and args.interval < 0.1:
        args.interval = 0.1
    config = Config(args.proc_root, args.sys_root, args.include_loopback,
                    args.include_virtual_disks, args.include_partitions)
    stopped = False
    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    previous = None
    iterations = 0
    output_format = "framed" if args.framed else args.format
    try:
        while not stopped:
            current = snapshot(config)
            report = build_report(current, previous, args.warning, args.critical)
            if output_format in ("json", "jsonl"):
                print(json.dumps(report, indent=2 if output_format == "json" else None,
                                 ensure_ascii=False, allow_nan=False), flush=True)
            elif output_format == "framed":
                print(framed(report, args.width, args.details), flush=True)
            else:
                print(line_format(report), flush=True)
            if args.csv:
                write_csv(args.csv, report)
            if args.jsonl_file:
                write_jsonl(args.jsonl_file, report)
            previous = current
            iterations += 1
            if args.once or args.interval == 0 or (args.count and iterations >= args.count):
                break
            deadline = current["monotonic"] + args.interval
            while not stopped:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(remaining, 0.1))
    except (OSError, BrokenPipeError) as exc:
        print(f"perf-check: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
