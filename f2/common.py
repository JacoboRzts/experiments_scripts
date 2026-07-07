#!/usr/bin/env python3
"""
F2 Common - Funciones compartidas entre S1 y S2
"""

import json
import statistics
from pathlib import Path

LINE_RATE_BPS = 1_000_000_000
DURATION = 20
COOLDOWN = 5
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
INFLECTION_PCT = 15.0
INFLECTION_ABS_US = 500
OVERSUB_LEVELS = [0, 2, 4, 6, 8, 10, 12, 15, 20, 30, 50]
LINE_RATE_MBPS = 1000
PKT_SIZES = [1518, 512]
OUTPUT_BASE = Path.home() / "experimentos"

# --- statistics -------------------------------------------

def probe_rate_mbps(level):
    if level == 0:
        return 10
    return round(LINE_RATE_MBPS * level / 100)

def compute_rsd(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def detect_inflexion(level_results, baseline_lat):
    if len(level_results) < 2:
        return None
    prev = level_results[-2]
    curr = level_results[-1]
    prev_lat = prev.get("lat_max_us") or 0
    curr_lat = curr.get("lat_max_us") or 0
    base = baseline_lat or 0
    prev_delta = prev_lat - base
    curr_delta = curr_lat - base
    if prev_delta <= 0:
        return None
    delta_pct = abs(curr_delta - prev_delta) / prev_delta * 100
    if delta_pct < INFLECTION_PCT:
        return prev["level"]
    if (prev_delta > INFLECTION_ABS_US and curr_delta > INFLECTION_ABS_US
            and abs(curr_lat - prev_lat) < INFLECTION_ABS_US):
        return prev["level"]
    return None

def calculate_buffer(baseline_lat_us, inflexion_lat_us):
    if baseline_lat_us is None or inflexion_lat_us is None:
        return None
    buffer_lat_us = inflexion_lat_us - baseline_lat_us
    if buffer_lat_us <= 0:
        return None
    return round((buffer_lat_us / 1_000_000) * LINE_RATE_BPS / 8)

# --- measure ----------------------------------------------

def measure_baseline(executors, dry_run, target_ip):
    """Measure clean baseline latency using ping from H4 to target."""
    if dry_run:
        return 1200.0
    print("  Measuring clean baseline (no load)...", end="", flush=True)
    h4 = executors["H4"]
    cmd = f"ping -c 20 -i 0.2 {target_ip} 2>&1"
    res = h4.run(cmd, timeout=30)
    for line in res.stdout.splitlines():
        if "min/avg/max" in line or "rtt" in line.lower():
            try:
                parts = line.split("=")[1].strip().split("/")
                lat_max = float(parts[2]) * 1000
                lat_avg = float(parts[1]) * 1000
                print(f" avg={lat_avg:.0f}us max={lat_max:.0f}us")
                return lat_max
            except (IndexError, ValueError):
                pass
    print(" failed to measure baseline, using None")
    return None

def run_main_flow(executors, pkt_size, results, errors, dry_run, target_ip):
    if dry_run:
        results["throughput_mbps"] = 999.0
        return
    h1 = executors["H1"]
    cmd = f"iperf3 -c {target_ip} -p 5201 -t {DURATION} -b 0 -M {pkt_size} -J"
    res = h1.run(cmd, timeout=DURATION+20)
    if res.returncode != 0 or not res.stdout.strip():
        errors.append(f"main flow rc={res.returncode}")
        results["throughput_mbps"] = None
        return
    try:
        data = json.loads(res.stdout)
        bps = data["end"]["sum_sent"]["bits_per_second"]
        results["throughput_mbps"] = round(bps / 1e6, 2)
    except Exception as e:
        errors.append(str(e))
        results["throughput_mbps"] = None

def run_probe_flow(executors, pkt_size, level, protocol, results, errors, dry_run, target_ip):
    if dry_run:
        results["throughput_mbps"] = probe_rate_mbps(level) * 0.95
        results["retransmits"] = 0
        return
    h4 = executors["H4"]
    rate_mbps = probe_rate_mbps(level)
    proto_flag = "-u" if protocol == "udp" else ""
    cmd = f"iperf3 -c {target_ip} -p 5202 -t {DURATION} {proto_flag} -b {rate_mbps}M -l {pkt_size} -J"
    res = h4.run(cmd, timeout=DURATION+20)
    if res.returncode != 0 or not res.stdout.strip():
        errors.append(f"probe rc={res.returncode}")
        results["throughput_mbps"] = None
        results["retransmits"] = None
        return
    try:
        data = json.loads(res.stdout)
        end = data["end"]
        if protocol == "udp":
            results["throughput_mbps"] = round(end["sum"]["bits_per_second"] / 1e6, 2)
            results["lost_packets"] = end["sum"].get("lost_packets", 0)
            results["retransmits"] = 0
        else:
            results["throughput_mbps"] = round(end["sum_sent"]["bits_per_second"] / 1e6, 2)
            results["retransmits"] = end["sum_sent"].get("retransmits", 0)
            results["lost_packets"] = 0
    except Exception as e:
        errors.append(str(e))
        results["throughput_mbps"] = None
        results["retransmits"] = None

def run_extra_flow(executors, host_key, port, pkt_size, protocol, rate_mbps, results, errors, dry_run, target_ip):
    if dry_run:
        results["throughput_mbps"] = rate_mbps * 0.9
        return
    h = executors[host_key]
    proto_flag = "-u" if protocol == "udp" else ""
    cmd = f"iperf3 -c {target_ip} -p {port} -t {DURATION} {proto_flag} -b {rate_mbps}M -l {pkt_size} -J"
    res = h.run(cmd, timeout=DURATION+20)
    if res.returncode != 0 or not res.stdout.strip():
        errors.append(f"{host_key} extra flow rc={res.returncode}")
        results["throughput_mbps"] = None
        return
    try:
        data = json.loads(res.stdout)
        end = data["end"]
        if protocol == "udp":
            results["throughput_mbps"] = round(end["sum"]["bits_per_second"] / 1e6, 2)
        else:
            results["throughput_mbps"] = round(end["sum_sent"]["bits_per_second"] / 1e6, 2)
    except Exception as e:
        errors.append(str(e))
        results["throughput_mbps"] = None

def run_ping_client(executors, duration, results, errors, dry_run, target_ip):
    if dry_run:
        results["lat_avg_us"] = 500.0
        results["lat_max_us"] = 800.0
        return
    h4 = executors["H4"]
    count = max(10, duration * 5)
    cmd = f"ping -c {count} -i 0.2 {target_ip} 2>&1"
    proc = h4.popen(cmd)
    try:
        stdout, stderr = proc.communicate(timeout=duration+20)
        out = stdout.decode() if isinstance(stdout, bytes) else stdout
        lat_avg = lat_max = None
        for line in out.splitlines():
            if "min/avg/max" in line or "rtt" in line.lower():
                try:
                    parts = line.split("=")[1].strip().split("/")
                    lat_avg = float(parts[1]) * 1000
                    lat_max = float(parts[2]) * 1000
                except (IndexError, ValueError):
                    pass
        if lat_avg is None:
            errors.append("ping: could not parse RTT")
            results["lat_avg_us"] = None
            results["lat_max_us"] = None
            return
        results["lat_avg_us"] = round(lat_avg, 2)
        results["lat_max_us"] = round(lat_max, 2)
    except Exception as e:
        errors.append(f"ping exception: {e}")
        results["lat_avg_us"] = None
        results["lat_max_us"] = None
    finally:
        h4.kill_process(proc)
