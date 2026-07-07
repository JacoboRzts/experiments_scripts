#!/usr/bin/env python3
"""
statistics.py - Common statistical functions for network experiments
"""

import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any


def compute_rsd(values: List[float]) -> Optional[float]:
    """
    Compute Relative Standard Deviation (RSD) as percentage
    RSD = (std_dev / mean) * 100
    """
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100


def safe_stats(vals: List[float]) -> tuple:
    """
    Compute min, mean, max, std, rsd safely
    Returns: (min, mean, max, std, rsd)
    """
    clean = [v for v in vals if v is not None]
    if not clean:
        return None, None, None, None, None
    
    min_val = round(min(clean), 4)
    mean_val = round(statistics.mean(clean), 4)
    max_val = round(max(clean), 4)
    std_val = round(statistics.stdev(clean), 4) if len(clean) > 1 else None
    rsd_val = round(compute_rsd(clean) or 0, 2)
    
    return min_val, mean_val, max_val, std_val, rsd_val


def extract_mbps(data: dict) -> Optional[float]:
    """Extract throughput in Mbps from iperf3 JSON output"""
    try:
        # For TCP - sum_sent
        if "end" in data and "sum_sent" in data["end"]:
            return data["end"]["sum_sent"]["bits_per_second"] / 1e6
        # For UDP - sum
        elif "end" in data and "sum" in data["end"]:
            return data["end"]["sum"].get("bits_per_second", 0) / 1e6
        # Alternative format
        elif "end" in data and "sum_received" in data["end"]:
            return data["end"]["sum_received"]["bits_per_second"] / 1e6
    except (KeyError, TypeError):
        return None
    return None


def extract_jitter(data: dict) -> Optional[float]:
    """Extract jitter in ms from iperf3 JSON output (UDP only)"""
    try:
        if "end" in data and "sum" in data["end"]:
            return data["end"]["sum"].get("jitter_ms")
    except (KeyError, TypeError):
        return None
    return None


def extract_cwnd(data: dict) -> tuple:
    """
    Extract congestion window statistics from iperf3 JSON
    Returns: (cwnd_avg, cwnd_max)
    """
    snd_cwnd_values = []
    try:
        intervals = data.get("intervals", [])
        for interval in intervals:
            cwnd = interval.get("sum", {}).get("snd_cwnd")
            if cwnd is not None:
                snd_cwnd_values.append(cwnd)
    except (KeyError, ValueError, TypeError):
        pass
    
    cwnd_avg = round(statistics.mean(snd_cwnd_values), 0) if snd_cwnd_values else None
    cwnd_max = round(max(snd_cwnd_values), 0) if snd_cwnd_values else None
    return cwnd_avg, cwnd_max


def inject_meta(data: dict, meta_info: Dict[str, Any]) -> dict:
    """
    Inject metadata into iperf3 result dictionary
    
    Expected meta_info keys:
    - experiment: str
    - topology: str
    - pair_id: str (optional)
    - client_host: str (optional)
    - server_host: str (optional)
    - pkt_size_b: int
    - rep: int
    - duration_s: int
    - cooldown_s: int
    - protocol: str (tcp/udp)
    - tcp_congestion_control: str (optional)
    - rfc_reference: str
    - note: str (optional)
    - extra: dict (optional)
    """
    # Extract cwnd for TCP if available
    cwnd_avg, cwnd_max = extract_cwnd(data)
    
    # Build metadata
    meta = {
        "experiment": meta_info.get("experiment"),
        "topology": meta_info.get("topology"),
        "pkt_size_b": meta_info.get("pkt_size_b"),
        "rep": meta_info.get("rep"),
        "duration_s": meta_info.get("duration_s"),
        "cooldown_s": meta_info.get("cooldown_s"),
        "protocol": meta_info.get("protocol", "tcp"),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rfc_reference": meta_info.get("rfc_reference"),
        "snd_cwnd_avg_bytes": cwnd_avg,
        "snd_cwnd_max_bytes": cwnd_max,
    }
    
    # Add optional fields
    if "pair_id" in meta_info:
        meta["pair_id"] = meta_info["pair_id"]
    if "client_host" in meta_info:
        meta["client_host"] = meta_info["client_host"]
    if "server_host" in meta_info:
        meta["server_host"] = meta_info["server_host"]
    if "tcp_congestion_control" in meta_info:
        meta["tcp_congestion_control"] = meta_info["tcp_congestion_control"]
    if "note" in meta_info:
        meta["note"] = meta_info["note"]
    if "extra" in meta_info:
        meta.update(meta_info["extra"])
    
    data["_meta"] = meta
    return data


def nombre_archivo(prefix: str, suffix: str, extension: str = "json") -> str:
    """Generate consistent filename"""
    return f"{prefix}_{suffix}.{extension}"


def save_summary(summary_path: Path, data: dict):
    """Save summary data to JSON file"""
    summary_path.write_text(json.dumps(data, indent=2))


def load_json_file(filepath: Path) -> Optional[dict]:
    """Load JSON file safely"""
    try:
        return json.loads(filepath.read_text())
    except (json.JSONDecodeError, FileNotFoundError):
        return None
