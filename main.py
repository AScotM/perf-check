#!/usr/bin/env python3

import os
import time
import sys
import signal
import argparse
import socket
import csv
from typing import Dict, Optional, Tuple

running = True


def signal_handler(_sig, _frame) -> None:
    global running
    running = False


def read_load() -> Dict[str, float]:
    try:
        with open("/proc/loadavg", "r", encoding="utf-8") as f:
            parts = f.read().split()
            return {
                "load1": float(parts[0]),
                "load5": float(parts[1]),
                "load15": float(parts[2]),
            }
    except (FileNotFoundError, IndexError, ValueError, OSError):
        return {
            "load1": 0.0,
            "load5": 0.0,
            "load15": 0.0,
        }


def read_mem() -> Dict[str, int]:
    result: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if ":" not in line:
                    continue
                key, val = line.split(":", 1)
                parts = val.strip().split()
                if parts:
                    try:
                        result[key] = int(parts[0]) * 1024
                    except ValueError:
                        continue
    except (FileNotFoundError, OSError):
        pass

    return {
        "mem_total": result.get("MemTotal", 0),
        "mem_available": result.get("MemAvailable", 0),
        "swap_total": result.get("SwapTotal", 0),
        "swap_free": result.get("SwapFree", 0),
    }


def read_vmstat() -> Dict[str, int]:
    result: Dict[str, int] = {}
    try:
        with open("/proc/vmstat", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        result[parts[0]] = int(parts[1])
                    except ValueError:
                        continue
    except (FileNotFoundError, OSError):
        pass

    return {
        "pswpin": result.get("pswpin", 0),
        "pswpout": result.get("pswpout", 0),
    }


def read_cpu() -> Dict[str, int]:
    try:
        with open("/proc/stat", "r", encoding="utf-8") as f:
            parts = f.readline().split()[1:]
            values = list(map(int, parts))
        if len(values) < 4:
            return {"idle": 0, "total": 0}
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        total = sum(values)
        return {"idle": idle, "total": total}
    except (FileNotFoundError, IndexError, ValueError, OSError):
        return {"idle": 0, "total": 0}


def calc_cpu(prev: Dict[str, int], curr: Dict[str, int]) -> float:
    idle_delta = curr["idle"] - prev["idle"]
    total_delta = curr["total"] - prev["total"]
    if total_delta <= 0:
        return 0.0
    usage = 100.0 * (1.0 - idle_delta / total_delta)
    return min(max(usage, 0.0), 100.0)


def read_diskstats() -> Dict[str, int]:
    read_ios = 0
    write_ios = 0
    try:
        with open("/proc/diskstats", "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 14:
                    continue
                dev = parts[2]
                if (
                    dev.startswith("loop")
                    or dev.startswith("ram")
                    or dev.startswith("dm-")
                    or dev.startswith("md")
                ):
                    continue
                try:
                    read_ios += int(parts[3])
                    write_ios += int(parts[7])
                except ValueError:
                    continue
    except (FileNotFoundError, OSError):
        pass
    return {"read_ios": read_ios, "write_ios": write_ios}


def read_netdev() -> Dict[str, int]:
    rx = 0
    tx = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8") as f:
            lines = f.readlines()[2:]
            for line in lines:
                if ":" not in line:
                    continue
                iface, data = line.split(":", 1)
                iface = iface.strip()
                if iface == "lo":
                    continue
                fields = data.split()
                if len(fields) >= 9:
                    try:
                        rx += int(fields[0])
                        tx += int(fields[8])
                    except ValueError:
                        continue
    except (FileNotFoundError, OSError, IndexError):
        pass
    return {"rx_bytes": rx, "tx_bytes": tx}


def read_snapshot() -> Dict[str, float | int | str]:
    data: Dict[str, float | int | str] = {}
    data.update(read_load())
    data.update(read_mem())
    data.update(read_vmstat())
    data.update(read_diskstats())
    data.update(read_netdev())
    data["timestamp"] = int(time.time())
    data["host"] = socket.gethostname()
    return data


def calc_rate(curr: int, prev: int, elapsed: float) -> float:
    if elapsed <= 0:
        return 0.0
    delta = curr - prev
    if delta < 0:
        return 0.0
    return delta / elapsed


def enrich_snapshot(
    current: Dict[str, float | int | str],
    previous: Optional[Dict[str, float | int | str]],
    prev_cpu: Optional[Dict[str, int]],
    curr_cpu: Dict[str, int],
    elapsed: float
) -> Dict[str, float | int | str]:
    data = dict(current)

    if prev_cpu is None:
        data["cpu_usage"] = 0.0
    else:
        data["cpu_usage"] = calc_cpu(prev_cpu, curr_cpu)

    if previous is None or elapsed <= 0:
        data["swapin_rate"] = 0.0
        data["swapout_rate"] = 0.0
        data["read_iops"] = 0.0
        data["write_iops"] = 0.0
        data["rx_rate"] = 0.0
        data["tx_rate"] = 0.0
    else:
        data["swapin_rate"] = calc_rate(int(data["pswpin"]), int(previous["pswpin"]), elapsed)
        data["swapout_rate"] = calc_rate(int(data["pswpout"]), int(previous["pswpout"]), elapsed)
        data["read_iops"] = calc_rate(int(data["read_ios"]), int(previous["read_ios"]), elapsed)
        data["write_iops"] = calc_rate(int(data["write_ios"]), int(previous["write_ios"]), elapsed)
        data["rx_rate"] = calc_rate(int(data["rx_bytes"]), int(previous["rx_bytes"]), elapsed)
        data["tx_rate"] = calc_rate(int(data["tx_bytes"]), int(previous["tx_bytes"]), elapsed)

    return data


def format_bytes(num: int) -> str:
    value = float(num)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    return f"{value:.1f} {units[idx]}"


def format_rate(value: float) -> str:
    units = ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"]
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    return f"{value:.2f} {units[idx]}"


def format_ops(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} M/s"
    if value >= 1_000:
        return f"{value / 1_000:.2f} K/s"
    return f"{value:.2f} /s"


def frame_top(width: int) -> str:
    return "┌" + "─" * (width - 2) + "┐"


def frame_mid(width: int) -> str:
    return "├" + "─" * (width - 2) + "┤"


def frame_bottom(width: int) -> str:
    return "└" + "─" * (width - 2) + "┘"


def frame_text(text: str, width: int) -> str:
    inner = width - 4
    if len(text) > inner:
        text = text[:inner]
    return f"│ {text.ljust(inner)} │"


def print_framed_report(data: Dict[str, float | int | str], elapsed: float, width: int = 118) -> None:
    mem_total = int(data["mem_total"])
    mem_available = int(data["mem_available"])
    swap_total = int(data["swap_total"])
    swap_free = int(data["swap_free"])

    mem_used = max(mem_total - mem_available, 0)
    swap_used = max(swap_total - swap_free, 0)

    mem_used_pct = (mem_used / mem_total * 100.0) if mem_total > 0 else 0.0
    swap_used_pct = (swap_used / swap_total * 100.0) if swap_total > 0 else 0.0

    title = (
        f"PERF CHECK  host={data['host']}  ts={data['timestamp']}  "
        f"elapsed={elapsed:.3f}s"
    )

    line1 = (
        f"CPU {float(data['cpu_usage']):6.2f}%   "
        f"LOAD {float(data['load1']):.2f}/{float(data['load5']):.2f}/{float(data['load15']):.2f}"
    )

    line2 = (
        f"MEM used={format_bytes(mem_used)} ({mem_used_pct:.1f}%)   "
        f"avail={format_bytes(mem_available)}   total={format_bytes(mem_total)}"
    )

    line3 = (
        f"SWAP used={format_bytes(swap_used)} ({swap_used_pct:.1f}%)   "
        f"free={format_bytes(swap_free)}   total={format_bytes(swap_total)}"
    )

    line4 = (
        f"SWAP IO in={format_ops(float(data['swapin_rate']))}   "
        f"out={format_ops(float(data['swapout_rate']))}"
    )

    line5 = (
        f"DISK IOPS read={format_ops(float(data['read_iops']))}   "
        f"write={format_ops(float(data['write_iops']))}"
    )

    line6 = (
        f"NET RX={format_rate(float(data['rx_rate']))}   "
        f"TX={format_rate(float(data['tx_rate']))}"
    )

    line7 = (
        f"TOTALS disk_io={int(data['read_ios'])}/{int(data['write_ios'])}   "
        f"net={int(data['rx_bytes'])}/{int(data['tx_bytes'])}   "
        f"swap={int(data['pswpin'])}/{int(data['pswpout'])}"
    )

    print(frame_top(width))
    print(frame_text(title, width))
    print(frame_mid(width))
    print(frame_text(line1, width))
    print(frame_text(line2, width))
    print(frame_text(line3, width))
    print(frame_text(line4, width))
    print(frame_text(line5, width))
    print(frame_text(line6, width))
    print(frame_text(line7, width))
    print(frame_bottom(width))


def format_line(data: Dict[str, float | int | str], elapsed: float) -> str:
    mem_available_mb = int(data["mem_available"]) // 1024 // 1024
    swap_free_mb = int(data["swap_free"]) // 1024 // 1024
    return (
        f"{data['timestamp']} {data['host']} "
        f"elapsed={elapsed:.3f}s "
        f"cpu={float(data['cpu_usage']):.2f}% "
        f"load={float(data['load1']):.2f}/{float(data['load5']):.2f}/{float(data['load15']):.2f} "
        f"mem_available={mem_available_mb}MB "
        f"swap_free={swap_free_mb}MB "
        f"swap_rate={float(data['swapin_rate']):.2f}/{float(data['swapout_rate']):.2f} "
        f"iops={float(data['read_iops']):.2f}/{float(data['write_iops']):.2f} "
        f"net_rate={float(data['rx_rate']):.2f}/{float(data['tx_rate']):.2f}"
    )


def ensure_csv_header(path: str) -> None:
    needs_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if not needs_header:
        return
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "host",
            "elapsed_s",
            "cpu_usage_pct",
            "load1",
            "load5",
            "load15",
            "mem_total_bytes",
            "mem_available_bytes",
            "swap_total_bytes",
            "swap_free_bytes",
            "pswpin_total",
            "pswpout_total",
            "swapin_rate_per_s",
            "swapout_rate_per_s",
            "read_ios_total",
            "write_ios_total",
            "read_iops",
            "write_iops",
            "rx_bytes_total",
            "tx_bytes_total",
            "rx_rate_Bps",
            "tx_rate_Bps",
        ])


def append_csv(path: str, data: Dict[str, float | int | str], elapsed: float) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            data["timestamp"],
            data["host"],
            f"{elapsed:.6f}",
            f"{float(data['cpu_usage']):.2f}",
            f"{float(data['load1']):.2f}",
            f"{float(data['load5']):.2f}",
            f"{float(data['load15']):.2f}",
            int(data["mem_total"]),
            int(data["mem_available"]),
            int(data["swap_total"]),
            int(data["swap_free"]),
            int(data["pswpin"]),
            int(data["pswpout"]),
            f"{float(data['swapin_rate']):.6f}",
            f"{float(data['swapout_rate']):.6f}",
            int(data["read_ios"]),
            int(data["write_ios"]),
            f"{float(data['read_iops']):.6f}",
            f"{float(data['write_iops']):.6f}",
            int(data["rx_bytes"]),
            int(data["tx_bytes"]),
            f"{float(data['rx_rate']):.6f}",
            f"{float(data['tx_rate']):.6f}",
        ])


def take_sample(
    previous_snapshot: Optional[Dict[str, float | int | str]],
    previous_cpu: Optional[Dict[str, int]],
    previous_time: Optional[float]
) -> Tuple[Dict[str, float | int | str], Dict[str, int], float]:
    current_time = time.time()
    current_snapshot = read_snapshot()
    current_cpu = read_cpu()

    if previous_time is None:
        elapsed = 0.0
    else:
        elapsed = max(current_time - previous_time, 0.0)

    data = enrich_snapshot(
        current=current_snapshot,
        previous=previous_snapshot,
        prev_cpu=previous_cpu,
        curr_cpu=current_cpu,
        elapsed=elapsed,
    )
    return data, current_cpu, current_time


def main() -> None:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Lightweight Linux performance snapshot utility")
    parser.add_argument("-i", "--interval", type=float, default=0.0)
    parser.add_argument("-c", "--count", type=int, default=0)
    parser.add_argument("--csv", type=str, default=None)
    parser.add_argument("--framed", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    min_interval = 0.1
    if args.interval < 0:
        print("Interval must be non-negative", file=sys.stderr)
        sys.exit(1)
    if 0 < args.interval < min_interval:
        args.interval = min_interval

    if args.csv:
        ensure_csv_header(args.csv)

    previous_snapshot: Optional[Dict[str, float | int | str]] = None
    previous_cpu: Optional[Dict[str, int]] = None
    previous_time: Optional[float] = None
    iteration = 0

    while running:
        data, current_cpu, current_time = take_sample(
            previous_snapshot=previous_snapshot,
            previous_cpu=previous_cpu,
            previous_time=previous_time,
        )

        elapsed = 0.0 if previous_time is None else max(current_time - previous_time, 0.0)

        if args.framed:
            print_framed_report(data, elapsed)
        else:
            print(format_line(data, elapsed))

        if args.csv:
            append_csv(args.csv, data, elapsed)

        previous_snapshot = read_snapshot()
        previous_cpu = current_cpu
        previous_time = current_time

        iteration += 1

        if args.once or args.interval <= 0:
            break

        if args.count > 0 and iteration >= args.count:
            break

        slept = 0.0
        while running and slept < args.interval:
            chunk = min(0.1, args.interval - slept)
            time.sleep(chunk)
            slept += chunk


if __name__ == "__main__":
    main()
