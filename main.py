#!/usr/bin/env python3

import os
import time
import argparse
import socket
from typing import Dict

def read_load() -> Dict[str, float]:
    with open("/proc/loadavg", "r") as f:
        parts = f.read().split()
        return {
            "load1": float(parts[0]),
            "load5": float(parts[1]),
            "load15": float(parts[2]),
        }

def read_mem() -> Dict[str, int]:
    result = {}
    with open("/proc/meminfo", "r") as f:
        for line in f:
            key, val = line.split(":", 1)
            result[key] = int(val.strip().split()[0]) * 1024
    return {
        "mem_total": result.get("MemTotal", 0),
        "mem_free": result.get("MemAvailable", 0),
        "swap_total": result.get("SwapTotal", 0),
        "swap_free": result.get("SwapFree", 0),
    }

def read_vmstat() -> Dict[str, int]:
    result = {}
    with open("/proc/vmstat", "r") as f:
        for line in f:
            k, v = line.split()
            result[k] = int(v)
    return {
        "pswpin": result.get("pswpin", 0),
        "pswpout": result.get("pswpout", 0),
    }

def read_cpu() -> Dict[str, float]:
    with open("/proc/stat", "r") as f:
        parts = f.readline().split()[1:]
        values = list(map(int, parts))
    idle = values[3]
    total = sum(values)
    return {"idle": idle, "total": total}

def calc_cpu(prev, curr) -> float:
    idle_delta = curr["idle"] - prev["idle"]
    total_delta = curr["total"] - prev["total"]
    if total_delta == 0:
        return 0.0
    return 100.0 * (1.0 - idle_delta / total_delta)

def read_diskstats() -> Dict[str, int]:
    read_ios = 0
    write_ios = 0
    with open("/proc/diskstats", "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            dev = parts[2]
            if dev.startswith("loop") or dev.startswith("ram"):
                continue
            read_ios += int(parts[3])
            write_ios += int(parts[7])
    return {"read_ios": read_ios, "write_ios": write_ios}

def read_netdev() -> Dict[str, int]:
    rx = 0
    tx = 0
    with open("/proc/net/dev", "r") as f:
        lines = f.readlines()[2:]
        for line in lines:
            iface, data = line.split(":", 1)
            iface = iface.strip()
            if iface == "lo":
                continue
            fields = data.split()
            rx += int(fields[0])
            tx += int(fields[8])
    return {"rx_bytes": rx, "tx_bytes": tx}

def snapshot(prev_cpu=None):
    data = {}
    data.update(read_load())
    data.update(read_mem())
    data.update(read_vmstat())
    data.update(read_diskstats())
    data.update(read_netdev())

    cpu_now = read_cpu()
    if prev_cpu:
        data["cpu_usage"] = calc_cpu(prev_cpu, cpu_now)
    else:
        data["cpu_usage"] = 0.0

    data["timestamp"] = int(time.time())
    data["host"] = socket.gethostname()

    return data, cpu_now

def format_line(d):
    return (
        f"{d['timestamp']} {d['host']} "
        f"cpu={d['cpu_usage']:.2f}% "
        f"load={d['load1']:.2f}/{d['load5']:.2f}/{d['load15']:.2f} "
        f"mem_free={d['mem_free']//1024//1024}MB "
        f"swap_free={d['swap_free']//1024//1024}MB "
        f"swap_io={d['pswpin']}/{d['pswpout']} "
        f"io={d['read_ios']}/{d['write_ios']} "
        f"net={d['rx_bytes']}/{d['tx_bytes']}"
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--interval", type=int, default=0)
    p.add_argument("-c", "--count", type=int, default=0)
    p.add_argument("--csv", type=str, default=None)
    args = p.parse_args()

    prev_cpu = None
    iteration = 0

    if args.csv:
        if not os.path.exists(args.csv):
            with open(args.csv, "w") as f:
                f.write(
                    "timestamp,host,cpu,load1,load5,load15,mem_free,swap_free,pswpin,pswpout,read_ios,write_ios,rx_bytes,tx_bytes\n"
                )

    while True:
        data, prev_cpu = snapshot(prev_cpu)

        print(format_line(data))

        if args.csv:
            with open(args.csv, "a") as f:
                f.write(
                    f"{data['timestamp']},{data['host']},{data['cpu_usage']:.2f},"
                    f"{data['load1']},{data['load5']},{data['load15']},"
                    f"{data['mem_free']},{data['swap_free']},"
                    f"{data['pswpin']},{data['pswpout']},"
                    f"{data['read_ios']},{data['write_ios']},"
                    f"{data['rx_bytes']},{data['tx_bytes']}\n"
                )

        if args.interval <= 0:
            break

        iteration += 1
        if args.count > 0 and iteration >= args.count:
            break

        time.sleep(args.interval)

if __name__ == "__main__":
    main()
