#!/usr/bin/env python3
"""
F2-S1 Mininet
"""

import statistics
import json
import threading
import time
from datetime import datetime, timezone

from f2.common import (
    DURATION, COOLDOWN, RSD_TARGET, REPS_MIN, REPS_MAX,
    OVERSUB_LEVELS, PKT_SIZES, OUTPUT_BASE, LINE_RATE_BPS, INFLECTION_PCT,
    probe_rate_mbps, compute_rsd, detect_inflexion, calculate_buffer,
    measure_baseline, run_main_flow, run_probe_flow, run_ping_client
)

EXPERIMENT = "f2s1"

def run_protocol(executors, topology, protocol, pkt_size, out_dir, dry_run):
    print(f"  --- {protocol.upper()} / pkt {pkt_size}B ---")
    
    # Obtener IP dinámica de H7
    target_ip = executors["H7"].IP()
    
    baseline_lat = measure_baseline(executors, dry_run, target_ip)
    level_summaries = []
    inflexion_level = None
    inflexion_lat = None

    for level in OVERSUB_LEVELS:
        rate_mbps = probe_rate_mbps(level)
        print(f"\n  Level {level:2d}% (+{rate_mbps:4d}M) ", end="", flush=True)
        lat_maxes = []
        lat_avgs = []
        rep = 0
        converged = False

        while rep < REPS_MAX:
            rep += 1
            if rep > 1:
                for i in range(COOLDOWN, 0, -1):
                    print(f"\r  Level {level:2d}% rep {rep:02d}/{REPS_MAX} cooling {i}s...  ", end="", flush=True)
                    time.sleep(1)
            print(f"\r  Level {level:2d}% rep {rep:02d}/{REPS_MAX} measuring...       ", end="", flush=True)

            res_main = {}
            err_main = []
            res_probe = {}
            err_probe = []
            res_lat = {}
            err_lat = []

            t_main = threading.Thread(target=run_main_flow, args=(executors, pkt_size, res_main, err_main, dry_run, target_ip))
            t_probe = threading.Thread(target=run_probe_flow, args=(executors, pkt_size, level, protocol, res_probe, err_probe, dry_run, target_ip))
            t_lat = threading.Thread(target=run_ping_client, args=(executors, DURATION, res_lat, err_lat, dry_run, target_ip))

            t_main.start()
            t_probe.start()
            t_lat.start()
            t_main.join(timeout=DURATION+30)
            t_probe.join(timeout=DURATION+30)
            t_lat.join(timeout=DURATION+30)

            for e in err_main + err_probe + err_lat:
                print(f"\n    WARN: {e}", end="")

            lat_max = res_lat.get("lat_max_us")
            if lat_max is not None:
                lat_maxes.append(lat_max)
            lat_avg = res_lat.get("lat_avg_us")
            if lat_avg is not None:
                lat_avgs.append(lat_avg)

            rsd = compute_rsd(lat_maxes) if len(lat_maxes) > 1 else 99.0
            lat_str = f"{lat_max:.1f}us" if lat_max else "N/A"
            rsd_str = f"{rsd:.1f}%" if rsd is not None else "N/A"
            print(f"\r  Level {level:2d}% rep {rep:02d}/{REPS_MAX} lat_max={lat_str} RSD={rsd_str}   ", end="", flush=True)

            # Save JSON
            fname = f"{topology}_{EXPERIMENT}_{protocol}_pkt{pkt_size:04d}_lvl{level:02d}_rep{rep:02d}.json"
            fpath = out_dir / fname
            record = {
                "_meta": {
                    "experiment": EXPERIMENT,
                    "topology": topology,
                    "scenario": "s1",
                    "protocol": protocol,
                    "pkt_size_b": pkt_size,
                    "level_pct": level,
                    "probe_rate_mbps": rate_mbps,
                    "rep": rep,
                    "duration_s": DURATION,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "target_ip": target_ip,
                },
                "main_flow": res_main,
                "probe_flow": res_probe,
                "latency": res_lat,
            }
            if not dry_run:
                fpath.write_text(json.dumps(record, indent=2))

            if rep >= REPS_MIN and rsd is not None and rsd < RSD_TARGET:
                converged = True
                break

        lat_max_avg = statistics.mean(lat_maxes) if lat_maxes else None
        lat_avg_avg = statistics.mean(lat_avgs) if lat_avgs else None
        level_summaries.append({
            "level": level,
            "lat_avg_us": round(lat_avg_avg, 2) if lat_avg_avg else None,
            "lat_max_us": round(lat_max_avg, 2) if lat_max_avg else None,
            "reps": rep,
            "rsd_pct": round(rsd, 2) if rsd is not None else None,
            "converged": converged,
        })

        lat_str = f"{lat_max_avg:.1f}us" if lat_max_avg else "N/A"
        print(f"\n  -> {rep} reps | lat_max={lat_str} | {'converged' if converged else 'no converge'}")

        if inflexion_level is None and baseline_lat is not None:
            inf = detect_inflexion(level_summaries, baseline_lat)
            if inf is not None:
                inflexion_level = inf
                inflexion_lat = level_summaries[-2]["lat_max_us"]
                buffer_bytes = calculate_buffer(baseline_lat, inflexion_lat)
                print(f"\n  Inflection detected at level {inflexion_level}%")
                if buffer_bytes:
                    print(f"  Buffer: {buffer_bytes:,} bytes ({buffer_bytes/1024:.1f} KB)")
                break

    buffer_bytes = calculate_buffer(baseline_lat, inflexion_lat)
    return {
        "protocol": protocol,
        "pkt_size_b": pkt_size,
        "baseline_lat_us": round(baseline_lat, 2) if baseline_lat else None,
        "inflexion_level": inflexion_level,
        "inflexion_lat_us": round(inflexion_lat, 2) if inflexion_lat else None,
        "buffer_bytes": buffer_bytes,
        "buffer_kb": round(buffer_bytes / 1024, 1) if buffer_bytes else None,
        "levels": level_summaries,
    }

def run_scenario_s1(executors, topology, protocols, dry_run):
    import statistics
    out_dir = OUTPUT_BASE / topology / "fase2_buffering"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*65}")
    print(f"  F2-S1  |  {topology.upper()}  |  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Scenario: H1+H4 -> H7 (min cross-leaf congestion)")
    print(f"  Protocols: {', '.join(p.upper() for p in protocols)}")
    print(f"  Levels: {OVERSUB_LEVELS}")
    print(f"  Packet sizes: {PKT_SIZES} bytes")
    print(f"  Output: {out_dir}")
    if dry_run:
        print("  DRY-RUN mode")
    print(f"{'='*65}\n")

    all_results = []

    for protocol in protocols:
        print(f"\n{'─'*65}")
        print(f"  Protocol: {protocol.upper()}")
        print(f"{'─'*65}")

        # Start iperf servers on H7
        if not dry_run:
            h7 = executors["H7"]
            h7.kill_iperf()
            time.sleep(0.5)
            h7.popen("iperf3 -s -p 5201")
            h7.popen("iperf3 -s -p 5202")
            time.sleep(2)

        for pkt_size in PKT_SIZES:
            result = run_protocol(executors, topology, protocol, pkt_size, out_dir, dry_run)
            all_results.append(result)

        # Kill servers
        if not dry_run:
            executors["H7"].kill_iperf()

        if protocol != protocols[-1]:
            print("  Pause 15s before next protocol...")
            if not dry_run:
                time.sleep(15)

    # Summary
    print(f"\n\n{'='*65}")
    print(f"  SUMMARY F2-S1 - {topology.upper()}")
    print(f"  Proto   PKT     Baseline  Inflection  Buffer")
    print(f"  -----   ---     --------  ----------  ------")
    for r in all_results:
        buf_str = f"{r['buffer_kb']} KB" if r["buffer_kb"] else "none"
        inf_str = f"{r['inflexion_level']}%" if r["inflexion_level"] is not None else "N/A"
        base_str = f"{r['baseline_lat_us']}us" if r["baseline_lat_us"] else "N/A"
        print(f"  {r['protocol'].upper():5}  {r['pkt_size_b']:3}B  {base_str:>10}  {inf_str:>10}  {buf_str:>12}")
    print("="*65)

    summary_path = out_dir / f"{topology}_{EXPERIMENT}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment": EXPERIMENT,
        "topology": topology,
        "scenario": "s1",
        "line_rate_bps": LINE_RATE_BPS,
        "oversub_levels": OVERSUB_LEVELS,
        "pkt_sizes": PKT_SIZES,
        "inflection_threshold_pct": INFLECTION_PCT,
        "rsd_target": RSD_TARGET,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results": all_results,
    }, indent=2))
    print(f"  Summary saved: {summary_path}")
