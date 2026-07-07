#!/usr/bin/env python3
"""
f3.py - F3 Jitter Testing with Mininet
RFC 1889 / RFC 8239 - SDN Testbed Spine-Leaf vs 3-Layer Hierarchical
Usage: sudo python3 f3.py --topology sl
"""

import os
import argparse
import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from mininet.log import info, error, warn, setLogLevel
from mininet.cli import CLI

from mininet_helpers import (
    MininetExecutor, create_network, configure_tcp,
    preflight, set_verbose, VERBOSE
)
from stats import (
    compute_rsd, safe_stats, extract_jitter,
    inject_meta, save_summary, load_json_file
)

# Global config
if 'SHELL' not in os.environ:
    os.environ['SHELL'] = '/bin/bash'

EXPERIMENT = "f3"
LINE_RATE_MBPS = 1000

DURATION = 30
COOLDOWN = 15
RATE_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
PKT_SIZE = 1024       # UDP payload (approx 1518B on wire with Ethernet overhead)

UDP_RATES = [10, 50, 100, 500, 900]

OUTPUT_BASE = Path.home() / "experimentos"

# Host mapping (same as F1)
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

# Topology configurations (with manual balancing for SL)
TOPOLOGIES = {
    "sl": {
        "hosts": HOSTS,
        # Corrected: 3 pairs now cross different leaves
        # Covers all 3 leaf-pair combinations: L1-L2, L2-L3, L3-L1
        "emisores": ["H1", "H4", "H7"],
        "destino": {"H1": "H5", "H4": "H8", "H7": "H2"},
        "puertos": {"H1": 5301, "H4": 5302, "H7": 5303},
        "scenario_desc": "Spine crossing in all 3 leaf combinations: H1(L1)->H5(L2), H4(L2)->H8(L3), H7(L3)->H2(L1)",
    },
    "ft": {
        "hosts": HOSTS,
        "emisores": ["H1", "H2", "H3"],
        "destino": {"H1": "H8", "H2": "H8", "H3": "H8"},
        "puertos": {"H1": 5301, "H2": 5302, "H3": 5303},
        "scenario_desc": "H1+H2+H3 (Edge1) -> H8 (Edge2) - crossing Core1",
    }
}


def start_servers(executor: MininetExecutor, dry_run: bool):
    """Start UDP servers on destination hosts"""
    if dry_run:
        return
    destinos = set(SCENARIO["destino"].values())
    for destino in destinos:
        # Kill any existing iperf3 processes
        executor.kill_iperf(destino)
        time.sleep(0.5)
        
        # Start servers for each port on this destination
        for emisor, puerto in SCENARIO["puertos"].items():
            if SCENARIO["destino"][emisor] == destino:
                if VERBOSE:
                    info(f"Starting UDP server on {destino} port {puerto}\n")
                executor.run_bg(destino, f"iperf3 -s -p {puerto}")
    # Wait for servers to start and verify
    time.sleep(2)
    # Verify servers are running
    for destino in destinos:
        result = executor.run_cmd(destino, "pgrep -f 'iperf3 -s'")
        if result.stdout.strip():
            if VERBOSE:
                info(f"Server running on {destino}\n")
        else:
            warn(f"WARNING: No iperf3 server found on {destino}\n")
    info(f"UDP servers active on {', '.join(destinos)}\n")

def stop_servers(executor: MininetExecutor, dry_run: bool):
    """Stop UDP servers"""
    if not dry_run:
        destinos = set(SCENARIO["destino"].values())
        for destino in destinos:
            executor.kill_iperf(destino)

def nombre_archivo(topology: str, rate_mbps: int, rep: int) -> str:
    """Generate filename for experiment results"""
    return f"{topology}_{EXPERIMENT}_udp_rate{rate_mbps:04d}_rep{rep:02d}.json"

def run_rate(executor: MininetExecutor, topology: str, rate_mbps: int, out_dir: Path, dry_run: bool) -> dict:
    """Run jitter testing for a single rate"""
    emisores = SCENARIO["emisores"]
    jitters_by_host = {h: [] for h in emisores}
    jitters_avg = []

    rep = 0
    converged = False

    print(f"\n  -- Rate: {rate_mbps} Mbps "
          f"({(rate_mbps/LINE_RATE_MBPS)*100:.0f}% LR) --")

    while rep < REPS_MAX:
        rep += 1

        if rep > 1:
            for i in range(COOLDOWN, 0, -1):
                print(f"\r  Rep {rep:02d}/{REPS_MAX} cooling down {i}s...   ",
                      end="", flush=True)
                time.sleep(1)

        print(f"\r  Rep {rep:02d}/{REPS_MAX} measuring {rate_mbps}Mbps...  ",
              end="", flush=True)

        results = {}
        errors = []
        threads = [
            threading.Thread(target=run_udp_flow,
                             args=(executor, h, rate_mbps, results, errors, dry_run))
            for h in emisores
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=DURATION + 25)

        for e in errors:
            print(f"\n    WARN  {e}", end="")

        # Extract jitter from each sender
        jitter_vals = []
        for h in emisores:
            r = results.get(h)
            if r and r.get("jitter_ms") is not None:
                jitter_vals.append(r["jitter_ms"])
                jitters_by_host[h].append(r["jitter_ms"])

        if jitter_vals:
            rep_jitter_avg = statistics.mean(jitter_vals)
            jitters_avg.append(rep_jitter_avg)

        rsd = compute_rsd(jitters_avg) if len(jitters_avg) > 1 else 99.0
        j_str = f"{rep_jitter_avg:.3f}ms" if jitter_vals else "N/A"
        rsd_str = f"{rsd:.1f}%" if rsd is not None else "N/A"
        ok_str = "OK" if rsd < RSD_TARGET else "WARN"

        print(f"\r  Rep {rep:02d}/{REPS_MAX} "
              f"jitter_avg={j_str} "
              f"RSD={ok_str}{rsd_str}   ")

        # Save JSON
        fname = nombre_archivo(topology, rate_mbps, rep)
        record = {
            "_meta": {
                "experiment": EXPERIMENT,
                "topology": topology,
                "rate_mbps": rate_mbps,
                "line_rate_pct": round((rate_mbps / LINE_RATE_MBPS) * 100, 1),
                "pkt_size_b": PKT_SIZE,
                "rep": rep,
                "duration_s": DURATION,
                "cooldown_s": COOLDOWN,
                "protocol": "udp",
                "tcp_congestion_control": "N/A (UDP)",
                "snd_cwnd_avg_bytes": None,
                "snd_cwnd_max_bytes": None,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "rfc_reference": "RFC1889 jitter / RFC8239 Section F3",
                "emisores": emisores,
                "destino_por_emisor": SCENARIO["destino"],
            },
            **{h: results.get(h) for h in emisores},
        }
        if not dry_run:
            (out_dir / fname).write_text(json.dumps(record, indent=2))

        if rep >= REPS_MIN and rsd is not None and rsd < RSD_TARGET:
            converged = True
            break

    if not converged:
        warn(f"RSD did not converge in {REPS_MAX} reps for {rate_mbps} Mbps\n")

    # Final statistics
    j_min, j_avg, j_max, j_std, j_rsd = safe_stats(jitters_avg)
    j_p95 = round(sorted(jitters_avg)[int(len(jitters_avg) * 0.95)], 4) if jitters_avg else None

    info(f"jitter min/avg/max/p95: {j_min}/{j_avg}/{j_max}/{j_p95} ms | RSD: {j_rsd}% | reps: {rep}\n")

    result = {
        "rate_mbps": rate_mbps,
        "lr_pct": round((rate_mbps / LINE_RATE_MBPS) * 100, 1),
        "reps": rep,
        "converged": converged,
        "jitter_min_ms": j_min,
        "jitter_avg_ms": j_avg,
        "jitter_max_ms": j_max,
        "jitter_std_ms": j_std,
        "jitter_p95_ms": j_p95,
        "jitter_rsd_pct": j_rsd,
    }
    for h in emisores:
        vals = jitters_by_host[h]
        result[f"jitter_{h.lower()}_avg"] = round(statistics.mean(vals), 4) if vals else None

    return result

def run_udp_flow(executor: MininetExecutor, host_key: str, rate_mbps: int, results: dict, errors: list, dry_run: bool):
    """
    Run UDP iperf3 flow from host_key to its configured destination
    """
    if dry_run:
        results[host_key] = {
            "jitter_ms": round(0.1 + rate_mbps * 0.0001, 4),
            "lost_packets": 0,
            "throughput_mbps": rate_mbps * 0.98,
        }
        return

    destino = SCENARIO["destino"][host_key]
    ip_destino = HOSTS[destino]["ip"]
    puerto = SCENARIO["puertos"][host_key]

    cmd = (
        f"iperf3 -c {ip_destino} -p {puerto}"
        f" -u -b {rate_mbps}M"
        f" -l {PKT_SIZE}"
        f" -t {DURATION}"
        f" -J"
    )

    try:
        res = executor.run_cmd(host_key, cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            # Get more error details
            error_detail = f"{host_key}: iperf3 rc={res.returncode}"
            if res.stderr:
                error_detail += f", stderr={res.stderr.strip()}"
            errors.append(error_detail)
            
            # Try to check if server is reachable
            if VERBOSE:
                ping_cmd = f"ping -c 1 -W 1 {ip_destino}"
                ping_res = executor.run_cmd(host_key, ping_cmd)
                if ping_res.returncode != 0:
                    info(f"  {host_key} cannot reach {ip_destino}\n")
                else:
                    info(f"  {host_key} can reach {ip_destino} but iperf3 failed\n")
            
            results[host_key] = None
            return

        data = json.loads(res.stdout)
        end = data.get("end", {})
        s = end.get("sum", {})

        jitter_ms = s.get("jitter_ms")
        lost_packets = s.get("lost_packets", 0)
        throughput_bps = s.get("bits_per_second", 0)

        results[host_key] = {
            "jitter_ms": round(jitter_ms, 4) if jitter_ms is not None else None,
            "lost_packets": lost_packets,
            "throughput_mbps": round(throughput_bps / 1e6, 2),
        }

    except json.JSONDecodeError as e:
        errors.append(f"{host_key}: Invalid JSON - {e}")
        if VERBOSE and res.stdout:
            info(f"  Output: {res.stdout[:200]}\n")
        results[host_key] = None
    except Exception as e:
        errors.append(f"{host_key}: error - {e}")
        results[host_key] = None
        executor.kill_iperf(host_key)

def run_experiment(executor: MininetExecutor, topology: str, rates: list, dry_run: bool) -> None:
    """Run complete F3 experiment"""
    out_dir = OUTPUT_BASE / topology / "fase3_jitter"
    out_dir.mkdir(parents=True, exist_ok=True)

    emisores_str = " + ".join(f"{h} ({HOSTS[h]['ip']}->{SCENARIO['destino'][h]})" 
                             for h in SCENARIO["emisores"])
    print(f"\n{'='*65}")
    print(f"  F3 JITTER  |  {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Scenario: {SCENARIO['desc']}")
    print(f"  Senders:  {emisores_str}")
    if topology == "sl":
        print(f"  Spine crossing: H1(L1)->H5(L2), H4(L2)->H8(L3), H7(L3)->H2(L1)")
    print(f"  Rates:     {rates} Mbps")
    print(f"  Cooldown:  {COOLDOWN}s")
    print(f"  Duration:  {DURATION}s per run")
    print(f"  Reps:      {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Output:    {out_dir}")
    if dry_run:
        print("  MODE:      DRY-RUN")
    print(f"{'='*65}\n")

    # Test connectivity before starting
    if not dry_run:
        info("Testing connectivity...\n")
        all_ok = True
        for emisor in SCENARIO["emisores"]:
            destino = SCENARIO["destino"][emisor]
            ip_destino = HOSTS[destino]["ip"]
            ping_cmd = f"ping -c 1 -W 1 {ip_destino}"
            ping_res = executor.run_cmd(emisor, ping_cmd)
            if ping_res.returncode == 0:
                info(f"  OK {emisor} -> {destino} ({ip_destino})\n")
            else:
                warn(f"  FAIL {emisor} -> {destino} ({ip_destino}) - No connectivity\n")
                all_ok = False
        
        if not all_ok:
            warn("Connectivity issues detected. Check Mininet topology.\n")

    start_servers(executor, dry_run)

    all_results = []
    for i, rate in enumerate(rates):
        result = run_rate(executor, topology, rate, out_dir, dry_run)
        all_results.append(result)
        if i < len(rates) - 1 and not dry_run:
            time.sleep(RATE_PAUSE)

    stop_servers(executor, dry_run)

    # ... rest of summary code ...

def main():
    parser = argparse.ArgumentParser(
        description="F3: Jitter Testing Unified - WITH MANUAL BALANCING"
    )
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl",
                        help="Topology: sl (spine-leaf) or ft (fat-tree)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate execution without real tests")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip preflight verification")
    parser.add_argument("--cli", action="store_true",
                        help="Start Mininet CLI after configuration")
    parser.add_argument("--controller", type=str, default="172.17.0.2",
                        help="Controller IP (e.g., 172.17.0.2)")
    parser.add_argument("--rates", nargs="+", type=int, default=UDP_RATES,
                        help="UDP rates in Mbps")
    parser.add_argument("--verbose", action="store_true",
                        help="Show verbose output including Mininet messages")
    args = parser.parse_args()

    # Set verbose mode
    set_verbose(args.verbose)

    global SCENARIO
    cfg = TOPOLOGIES[args.topology]
    SCENARIO = {
        "emisores": cfg["emisores"],
        "destino": cfg["destino"],
        "puertos": cfg["puertos"],
        "desc": cfg["scenario_desc"],
    }

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
        # Configure TCP even for UDP tests to ensure consistent host config
        configure_tcp(executor, "cubic")

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

        # Preflight check
        if not args.dry_run and not args.skip_preflight:
            all_hosts = list(SCENARIO["emisores"]) + list(set(SCENARIO["destino"].values()))
            if not preflight(executor, all_hosts):
                warn("Preflight failed. Check iperf3 before continuing.\n")

        run_experiment(
            executor=executor,
            topology=args.topology,
            rates=args.rates,
            dry_run=args.dry_run,
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
