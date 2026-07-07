#!/usr/bin/env python3
"""
f1.py - F1 Line-Rate Testing with Mininet
Usage: sudo python3 f1.py --experiment all --topology sl
"""

import os
import argparse
import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from mininet.log import info, error, warn, setLogLevel
from mininet.cli import CLI

from mininet_helpers import (
    MininetExecutor, create_network, configure_tcp, 
    preflight, set_verbose, VERBOSE
)
from statistics import (
    compute_rsd, inject_meta, extract_mbps, 
    save_summary, safe_stats
)

# Global config
if 'SHELL' not in os.environ:
    os.environ['SHELL'] = '/bin/bash'

DURATION = 30
COOLDOWN = 15
PKT_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
PKT_SIZES = [64, 128, 256, 512, 1024, 1518]
TCP_CONGESTION = "cubic"

OUTPUT_BASE = Path.home() / "experimentos"

# Host mapping
HOSTS = {
    "H1": {"ip": "10.0.1.1", "user": "root", "mininet_name": "h1"},
    "H2": {"ip": "10.0.1.2", "user": "root", "mininet_name": "h2"},
    "H3": {"ip": "10.0.1.3", "user": "root", "mininet_name": "h3"},
    "H4": {"ip": "10.0.2.1", "user": "root", "mininet_name": "h4"},
    "H5": {"ip": "10.0.2.2", "user": "root", "mininet_name": "h5"},
    "H6": {"ip": "10.0.2.3", "user": "root", "mininet_name": "h6"},
    "H7": {"ip": "10.0.3.1", "user": "root", "mininet_name": "h7"},
    "H8": {"ip": "10.0.3.2", "user": "root", "mininet_name": "h8"},
}

# Experiment's configurations
EXPERIMENTS = {
    "a1": {
        "desc": "Single pair (low load) - baseline",
        "note": "F1-A1: single pair H1->H4 - low load",
        "pairs": [
            {"id": "p1", "client": "H1", "server": "H4", "port": 5201},
        ]
    },
    "b1": {
        "desc": "2 cross-leaf pairs (partial full-mesh)",
        "note": "F1-B1: 2 simultaneous cross-leaf pairs - partial full-mesh",
        "pairs": [
            {"id": "p1", "client": "H2", "server": "H4", "port": 5201},
            {"id": "p2", "client": "H3", "server": "H7", "port": 5202},
        ]
    },
    "b2": {
        "desc": "4 cross-leaf pairs WITH manual load balancing",
        "note": "F1-B2: 4 full-mesh flows with manual balancing",
        "pairs": [
            {"id": "p1", "client": "H1", "server": "H4", "port": 5201},
            {"id": "p2", "client": "H2", "server": "H7", "port": 5202},
            {"id": "p3", "client": "H3", "server": "H5", "port": 5203},
            {"id": "p4", "client": "H6", "server": "H8", "port": 5204},
        ]
    },
}


def start_servers(executor: MininetExecutor, pairs: List[dict]) -> None:
    """Start iperf3 servers for all pairs"""
    hosts = {p["server"] for p in pairs}
    executor.kill_iperf_all(list(hosts))
    time.sleep(1)
    for p in pairs:
        executor.run_bg(p["server"], f"iperf3 -s -p {p['port']}")
    time.sleep(2)


def run_single_pair(executor: MininetExecutor, pair: dict, pkt_size: int,
                    rep: int, topology: str, experiment: str, note: str,
                    out_dir: Path, results: dict, errors: list,
                    dry_run: bool) -> None:
    """Run iperf3 for a single pair"""
    fname = (
        f"{topology}_{experiment}_tcp"
        f"_pkt{pkt_size:04d}"
        f"_{pair['id']}"
        f"_rep{rep:02d}.json"
    )
    fpath = out_dir / fname
    srv_ip = HOSTS[pair["server"]]["ip"]

    if dry_run:
        if VERBOSE:
            print(f"    [DRY] {pair['id']}: iperf3 -c {srv_ip} -p {pair['port']}"
                  f" -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J  ->  {fname}")
        results[pair["id"]] = None
        return

    cmd = (f"iperf3 -c {srv_ip} -p {pair['port']}"
           f" -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J")

    try:
        res = executor.run_cmd(pair["client"], cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            errors.append(f"{pair['id']}: iperf3 rc={res.returncode}, stderr={res.stderr}")
            results[pair["id"]] = None
            return

        try:
            data = json.loads(res.stdout)
        except json.JSONDecodeError as e:
            errors.append(f"{pair['id']}: JSON decode error: {e}")
            if VERBOSE:
                errors.append(f"Output snippet: {res.stdout[:200]}")
            results[pair["id"]] = None
            return

        # Inject metadata
        meta_info = {
            "experiment": experiment,
            "topology": topology,
            "pair_id": pair["id"],
            "client_host": pair["client"],
            "server_host": pair["server"],
            "pkt_size_b": pkt_size,
            "rep": rep,
            "duration_s": DURATION,
            "cooldown_s": COOLDOWN,
            "protocol": "tcp",
            "tcp_congestion_control": TCP_CONGESTION,
            "rfc_reference": "RFC8239 Section 2 - Line-Rate Testing (Mininet)",
            "note": note,
        }
        data = inject_meta(data, meta_info)
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = extract_mbps(data)

    except Exception as e:
        errors.append(f"{pair['id']}: {e}")
        results[pair["id"]] = None
        executor.kill_iperf(pair["client"])


def run_rep(executor: MininetExecutor, pairs: List[dict], pkt_size: int,
            rep: int, topology: str, experiment: str, note: str,
            out_dir: Path, dry_run: bool) -> List[Optional[float]]:
    """Run one repetition for all pairs in parallel"""
    results = {}
    errors = []
    threads = [
        threading.Thread(
            target=run_single_pair,
            args=(executor, p, pkt_size, rep, topology, experiment, note,
                  out_dir, results, errors, dry_run),
        )
        for p in pairs
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DURATION + 35)

    for err in errors:
        print(f"    WARN  {err}")

    return [results.get(p["id"]) for p in pairs]


def run_experiment(executor: MininetExecutor, experiment: str, topology: str,
                   dry_run: bool) -> None:
    """Run a complete experiment"""
    exp_config = EXPERIMENTS[experiment]
    pairs = exp_config["pairs"]
    note = exp_config["note"]
    desc = exp_config["desc"]

    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  F1-{experiment.upper()}  |  Topology: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Description: {desc}")
    print(f"  Cooldown:   {COOLDOWN}s  |  TCP: {TCP_CONGESTION}")
    print("  Pairs:  " + "  ".join(f"{p['id']}:{p['client']}->{p['server']}" for p in pairs))
    print(f"  Output: {out_dir}")
    print(f"  PKTs:   {PKT_SIZES}")
    print(f"  Reps:   {REPS_MIN}-{REPS_MAX}  (RSD < {RSD_TARGET}%)")
    if dry_run:
        print("  MODE:   DRY-RUN")
    print(f"{'='*70}\n")

    summary = {}

    for pkt_size in PKT_SIZES:
        print(f"-- PKT {pkt_size:>4d} B " + "-"*50)

        throughputs_per_rep = []

        if not dry_run:
            start_servers(executor, pairs)

        rep = 1
        converged = False
        while rep <= REPS_MAX:
            print(f"  Rep {rep:02d}/{REPS_MAX}  ", end="", flush=True)

            mbps_list = run_rep(executor, pairs, pkt_size, rep, topology,
                               experiment, note, out_dir, dry_run)
            valid = [v for v in mbps_list if v is not None]

            if valid:
                rep_mean = statistics.mean(valid)
                throughputs_per_rep.append(rep_mean)
                print(f"  {rep_mean:8.2f} Mbps  [{len(valid)}/{len(pairs)} ok]",
                      end="")
            else:
                print("  X all pairs failed", end="")

            if rep >= REPS_MIN and len(throughputs_per_rep) >= REPS_MIN:
                rsd = compute_rsd(throughputs_per_rep)
                if rsd is not None:
                    print(f"  RSD={rsd:.1f}%", end="")
                    if rsd < RSD_TARGET:
                        print("  OK converges")
                        converged = True
                        break

            print()
            if rep < REPS_MAX:
                time.sleep(COOLDOWN)
            rep += 1

        if not converged:
            print(f"\n  WARN  RSD did not converge in {REPS_MAX} reps")

        final_rsd = compute_rsd(throughputs_per_rep)
        final_mean = statistics.mean(throughputs_per_rep) if throughputs_per_rep else 0.0
        lr_pct = (final_mean / 1000.0) * 100

        summary[pkt_size] = {
            "reps": rep,
            "mean_mbps": round(final_mean, 2),
            "rsd_pct": round(final_rsd, 2) if final_rsd is not None else None,
            "lr_pct": round(lr_pct, 1),
        }

        rsd_str = f"{final_rsd:.1f}%" if final_rsd is not None else "N/A"
        print(f"  -> {rep} reps | {final_mean:.2f} Mbps | RSD={rsd_str} | {lr_pct:.1f}% LR\n")

        if not dry_run:
            hosts = {p["client"] for p in pairs} | {p["server"] for p in pairs}
            executor.kill_iperf_all(list(hosts))
            if pkt_size != PKT_SIZES[-1]:
                time.sleep(PKT_PAUSE)

    # Summary table
    print(f"\n{'='*70}")
    print(f"  SUMMARY F1-{experiment.upper()} -- {topology.upper()}")
    print(f"  Cooldown: {COOLDOWN}s | TCP: {TCP_CONGESTION}")
    print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
    for pkt, s in summary.items():
        rsd_str = f"{s['rsd_pct']:.1f}" if s["rsd_pct"] is not None else "N/A"
        print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}"
              f"  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
    print(f"{'='*70}\n")

    summary_path = out_dir / f"{topology}_{experiment}_summary.json"
    save_summary(summary_path, {
        "experiment": experiment,
        "topology": topology,
        "rfc_reference": "RFC8239 Section 2 (Mininet)",
        "desc": desc,
        "cooldown_s": COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "pairs": [{"id": p["id"], "client": p["client"],
                   "server": p["server"]} for p in pairs],
        "pkt_sizes": PKT_SIZES,
        "rsd_target": RSD_TARGET,
        "reps_min": REPS_MIN,
        "reps_max": REPS_MAX,
        "duration_s": DURATION,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results": {str(k): v for k, v in summary.items()},
    })
    print(f"  Summary: {summary_path}\n")


def main():
    parser = argparse.ArgumentParser(
        description="F1: Line-Rate Testing with Mininet"
    )
    parser.add_argument(
        "--experiment",
        choices=["a1", "b1", "b2", "all"],
        default="all",
        help="Experiment to run (default: all)"
    )
    parser.add_argument(
        "--topology",
        choices=["sl", "ft"],
        default="sl",
        help="Topology: sl (spine-leaf) or ft (fat-tree)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate execution without real tests")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip preflight verification")
    parser.add_argument("--cli", action="store_true",
                        help="Start Mininet CLI after configuration")
    parser.add_argument("--controller", type=str, default=None,
                        help="Controller IP (e.g., 172.17.0.2)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show verbose output including Mininet messages")
    args = parser.parse_args()

    # Set verbose mode
    set_verbose(args.verbose)

    if args.experiment == "all":
        experiments = ["a1", "b1", "b2"]
    else:
        experiments = [args.experiment]

    if not args.verbose:
        setLogLevel('warning')
    else:
        setLogLevel('info')

    if not args.verbose:
        info("Creating network...\n")
    else:
        info(f"Creating topology {args.topology.upper()}\n")
    
    net = create_network(args.topology, args.controller)

    try:
        if not args.verbose:
            info("Starting network...\n")
        else:
            info("Starting network\n")
        net.start()

        executor = MininetExecutor(net)

        if not args.verbose:
            info("Configuring TCP...\n")
        else:
            info("Configuring TCP\n")
        configure_tcp(executor, TCP_CONGESTION)

        # Verify iperf3 is available
        if not args.verbose:
            info("Checking iperf3...\n")
        else:
            info("Verifying iperf3\n")
        
        test_host = "H1"
        test_cmd = executor.run_cmd(test_host, "which iperf3 2>/dev/null || echo 'not found'")

        if "not found" in test_cmd.stdout or test_cmd.returncode != 0:
            warn("iperf3 not found. Installing on all hosts...\n")
            for host in net.hosts:
                host.cmd("apt-get update -qq 2>/dev/null && apt-get install -y iperf3 2>/dev/null || true")
                if "iperf3" not in host.cmd("which iperf3 2>/dev/null || echo 'not found'"):
                    host.cmd("yum install -y iperf3 2>/dev/null || true")

        # Run experiments
        for exp in experiments:
            if not args.dry_run and not args.skip_preflight:
                hosts = []
                for p in EXPERIMENTS[exp]["pairs"]:
                    hosts.extend([p["client"], p["server"]])
                hosts = list(set(hosts))
                if not preflight(executor, hosts):
                    warn(f"Preflight failed for {exp}. Check iperf3 before continuing.\n")
                    continue

            run_experiment(
                executor=executor,
                experiment=exp,
                topology=args.topology,
                dry_run=args.dry_run
            )

        if args.cli:
            info("Starting CLI (exit with 'exit')\n")
            CLI(net)

    except KeyboardInterrupt:
        info("\nInterrupted by user\n")
    except Exception as e:
        error(f"Error: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        info("Cleaning up...\n")
        if 'executor' in locals():
            executor.cleanup()
        net.stop()
        info("Done\n")


if __name__ == "__main__":
    main()
