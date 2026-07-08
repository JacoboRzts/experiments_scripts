#!/usr/bin/env python3
"""
f5.py - F5 Incast Testing with Mininet
RFC 8239 Section 6 - SDN Testbed Spine-Leaf vs 3-Layer Hierarchical
Usage: sudo python3 f5.py --topology sl
"""

import os
import argparse
import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

from mininet.log import info, error, warn, setLogLevel
from mininet.cli import CLI

from mininet_helpers import (
    MininetExecutor, create_network, configure_tcp,
    preflight, set_verbose, VERBOSE
)
from stats import (
    compute_rsd, inject_meta, extract_mbps, extract_jitter,
    save_summary, safe_stats
)

# Global config
if 'SHELL' not in os.environ:
    os.environ['SHELL'] = '/bin/bash'

EXPERIMENT = "f5"
LINE_RATE_MBPS = 1000

DURATION = 30
COOLDOWN = 15
RATE_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30

TCP_PAYLOAD = 1470  # Approx 1518B Ethernet frame
UDP_PAYLOAD = 1470
UDP_RATE = "100M"
TCP_CONGESTION = "cubic"

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
        "receptor": {"tcp": "H5", "udp": "H7"},
        "escenarios": {
            "s1": {
                "tcp_senders": ["H1", "H8"],
                "udp_sender": "H4",
                "tcp_receptor": "H5",
                "udp_receptor": "H7",
                "desc": "2 TCP senders (H1,L1->H8,L3) to H5(L2) + UDP H4(L2)->H7(L3)",
            },
            "s2": {
                "tcp_senders": ["H1", "H2", "H3", "H8"],
                "udp_sender": "H4",
                "tcp_receptor": "H5",
                "udp_receptor": "H7",
                "desc": "4 TCP senders (all cross-spine) to H5(L2) + UDP H4(L2)->H7(L3)",
            },
        },
        "scenario_desc": "Spine-Leaf: TCPs crossing leaf to H5 + UDP H4->H7 crossing leaf",
    },
    "ft": {
        "hosts": HOSTS,
        "receptor": {"tcp": "H5", "udp": "H5"},
        "escenarios": {
            "s1": {
                "tcp_senders": ["H1", "H4"],
                "udp_sender": "H2",
                "tcp_receptor": "H5",
                "udp_receptor": "H5",
                "desc": "2 TCP senders (Edge1) to H5(Edge2) + UDP H2->H5",
            },
            "s2": {
                "tcp_senders": ["H1", "H4", "H3"],
                "udp_sender": "H2",
                "tcp_receptor": "H5",
                "udp_receptor": "H5",
                "desc": "3 TCP senders (Edge1) to H5(Edge2) + UDP H2->H5",
            },
        },
        "scenario_desc": "3-Layer Hierarchical: Edge1=H1-H4, Edge2=H5-H8",
    }
}


def start_servers(executor: MininetExecutor, escenario_cfg: dict, dry_run: bool) -> List[int]:
    """Start TCP and UDP servers for the scenario"""

    num_senders = len(escenario_cfg["tcp_senders"])
    puertos = [5201 + i for i in range(num_senders)]
    
    if dry_run:
        return puertos

    # TCP servers on TCP receiver
    rx_tcp = escenario_cfg["tcp_receptor"]
    ip_rx_tcp = HOSTS[rx_tcp]["ip"]
    executor.kill_iperf(rx_tcp)
    time.sleep(2)


    for p in puertos:
        if VERBOSE:
            info(f"Starting TCP server on {rx_tcp} port {p}\n")
        executor.run_bg(rx_tcp, f"iperf3 -s -p {p} --bind {ip_rx_tcp}")
        time.sleep(0.5)

    # UDP server on UDP receiver
    rx_udp = escenario_cfg["udp_receptor"]
    ip_rx_udp = HOSTS[rx_udp]["ip"]
    executor.kill_iperf(rx_udp)
    time.sleep(2)

    if VERBOSE:
        info(f"Starting UDP server on {rx_udp} port 5400\n")
    executor.run_bg(rx_udp, f"iperf3 -s -p 5400 -u --bind {ip_rx_udp}")

    # Wait for servers to fully start
    time.sleep(3)

    # Verify servers are running (siempre, no solo en modo verbose,
    # para detectar fallos silenciosos de arranque)
    result = executor.run_cmd(rx_tcp, "pgrep -f 'iperf3 -s'")
    if result.stdout.strip():
        if VERBOSE:
            info(f"TCP server running on {rx_tcp}\n")
    else:
        warn(f"WARNING: No TCP server found on {rx_tcp}\n")

    result = executor.run_cmd(rx_udp, "pgrep -f 'iperf3 -s'")
    if result.stdout.strip():
        if VERBOSE:
            info(f"UDP server running on {rx_udp}\n")
    else:
        warn(f"WARNING: No UDP server found on {rx_udp}\n")

    info(f"Servers active: TCP on {rx_tcp}, UDP on {rx_udp}\n")
    return puertos


def stop_servers(executor: MininetExecutor, escenario_cfg: dict, dry_run: bool):
    """Stop all servers"""
    if dry_run:
        return

    hosts = {escenario_cfg["tcp_receptor"], escenario_cfg["udp_receptor"]}
    for h in hosts:
        executor.kill_iperf(h)
        time.sleep(1)


def run_tcp_flow(executor: MininetExecutor, sender: str, rx_ip: str, port: int, results: dict, errors: list, dry_run: bool):
    """Run TCP iperf3 flow from sender to receiver"""
    if dry_run:
        results[sender] = {
            "goodput_mbps": 950.0,
            "retransmits": 0,
        }
        return

    ip_origen = HOSTS[sender]["ip"]
    cmd = (
        f"iperf3 -c {rx_ip} -p {port}"
        f" -t {DURATION} -Z -C {TCP_CONGESTION}"
        f" -l {TCP_PAYLOAD}"
        f" --bind {ip_origen}"
        f" -J"
    )

    try:
        res = executor.run_cmd(sender, cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            error_detail = f"{sender}: TCP iperf3 rc={res.returncode}"
            if res.stderr:
                error_detail += f", stderr={res.stderr.strip()}"
            errors.append(error_detail)
            executor.kill_iperf(sender)
            results[sender] = None
            return

        data = json.loads(res.stdout)
        bps = data.get("end", {}).get("sum_received", {}).get("bits_per_second", 0)
        retransmits = data.get("end", {}).get("sum_sent", {}).get("retransmits", 0)

        results[sender] = {
            "goodput_mbps": bps / 1e6,
            "retransmits": retransmits,
        }

    except json.JSONDecodeError as e:
        errors.append(f"{sender}: TCP JSON decode error - {e}")
        results[sender] = None
    except Exception as e:
        errors.append(f"{sender}: TCP error - {e}")
        results[sender] = None
        executor.kill_iperf(sender)


def run_udp_flow(executor: MininetExecutor, sender: str, rx_ip: str, port: int, results: dict, errors: list, dry_run: bool):
    """Run UDP iperf3 flow from sender to receiver"""
    if dry_run:
        results[sender] = {
            "jitter_ms": 0.1,
            "lost_percent": 0.0,
        }
        return

    ip_origen = HOSTS[sender]["ip"]
    cmd = (
        f"iperf3 -c {rx_ip} -p {port} -u"
        f" -b {UDP_RATE}M"
        f" -l {UDP_PAYLOAD}"
        f" -t {DURATION}"
        f" --bind {ip_origen}"
        f" -J  2>/dev/null"
    )

    try:
        res = executor.run_cmd(sender, cmd, timeout=DURATION + 20)

        if res.returncode != 0 or not res.stdout.strip():
            error_detail = f"{sender}: UDP iperf3 rc={res.returncode}"
            if res.stderr:
                error_detail += f", stderr={res.stderr.strip()}"
            errors.append(error_detail)
            executor.kill_iperf(sender)
            results[sender] = None
            return

        data = json.loads(res.stdout)
        s = data.get("end", {}).get("sum", {})

        jitter_ms = s.get("jitter_ms", 0.0)
        lost_pct = s.get("lost_percent", 0.0)

        results[sender] = {
            "jitter_ms": jitter_ms,
            "lost_percent": lost_pct,
        }

    except json.JSONDecodeError as e:
        errors.append(f"{sender}: UDP JSON decode error - {e}")
        results[sender] = None
    except Exception as e:
        errors.append(f"{sender}: UDP error - {e}")
        results[sender] = None
        executor.kill_iperf(sender)


def run_rep(executor: MininetExecutor, escenario_cfg: dict, puertos: List[int],
            rep: int, topology: str, esc: str, out_dir: Path,
            dry_run: bool) -> Optional[Dict]:
    """Run one repetition for a scenario"""
    rx_tcp_ip = HOSTS[escenario_cfg["tcp_receptor"]]["ip"]
    rx_udp_ip = HOSTS[escenario_cfg["udp_receptor"]]["ip"]

    results = {}
    errors = []
    threads = []

    # Create TCP threads
    for i, sender in enumerate(escenario_cfg["tcp_senders"]):
        t = threading.Thread(
            target=run_tcp_flow,
            args=(executor, sender, rx_tcp_ip, puertos[i], results, errors, dry_run)
        )
        threads.append(t)

    # Create UDP thread
    udp_sender = escenario_cfg["udp_sender"]
    t = threading.Thread(
        target=run_udp_flow,
        args=(executor, udp_sender, rx_udp_ip, 5400, results, errors, dry_run)
    )
    threads.append(t)

    # Start all threads
    for t in threads:
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join(timeout=DURATION + 35)

    # Log errors
    for err in errors:
        print(f"    WARN  {err}")

    if dry_run:
        return {
            "goodput_by_sender": {},
            "goodput_aggregate_mbps": 0.0,
            "udp_jitter_ms": 0.0,
            "udp_lost_pct": 0.0,
            "retransmits": {},
            "ok": True
        }

    # Process results
    medicion = {
        "goodput_by_sender": {},
        "retransmits": {},
        "ok": True
    }

    # Process TCP results
    for sender in escenario_cfg["tcp_senders"]:
        r = results.get(sender)
        if r is None:
            medicion["ok"] = False
        else:
            medicion["goodput_by_sender"][sender] = r.get("goodput_mbps", 0.0)
            medicion["retransmits"][sender] = r.get("retransmits", 0)

    # Process UDP results
    udp_result = results.get(udp_sender)
    if udp_result is None:
        medicion["ok"] = False
        medicion["udp_jitter_ms"] = 0.0
        medicion["udp_lost_pct"] = 0.0
    else:
        medicion["udp_jitter_ms"] = udp_result.get("jitter_ms", 0.0)
        medicion["udp_lost_pct"] = udp_result.get("lost_percent", 0.0)

    medicion["goodput_aggregate_mbps"] = sum(medicion["goodput_by_sender"].values())

    # Save JSON for each flow
    for sender, g in medicion["goodput_by_sender"].items():
        fname = f"{topology}_{EXPERIMENT}{esc}_tcp_{sender}_{escenario_cfg['tcp_receptor']}_rep{rep:02d}.json"
        fpath = out_dir / fname
        record = {
            "_meta": {
                "experiment": EXPERIMENT,
                "scenario": esc,
                "topology": topology,
                "sender": sender,
                "receiver": escenario_cfg["tcp_receptor"],
                "rep": rep,
                "duration_s": DURATION,
                "cooldown_s": COOLDOWN,
                "protocol": "tcp",
                "tcp_congestion_control": TCP_CONGESTION,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "rfc_reference": "RFC8239 Section 6 - Incast Testing",
            },
            "goodput_mbps": g,
            "retransmits": medicion["retransmits"].get(sender, 0),
        }
        fpath.write_text(json.dumps(record, indent=2))

    # Save UDP result
    fname = f"{topology}_{EXPERIMENT}{esc}_udp_{udp_sender}_{escenario_cfg['udp_receptor']}_rep{rep:02d}.json"
    fpath = out_dir / fname
    record = {
        "_meta": {
            "experiment": EXPERIMENT,
            "scenario": esc,
            "topology": topology,
            "sender": udp_sender,
            "receiver": escenario_cfg["udp_receptor"],
            "rep": rep,
            "duration_s": DURATION,
            "cooldown_s": COOLDOWN,
            "protocol": "udp",
            "udp_rate": UDP_RATE,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "rfc_reference": "RFC8239 Section 6 - Incast Testing",
        },
        "jitter_ms": medicion["udp_jitter_ms"],
        "lost_percent": medicion["udp_lost_pct"],
    }
    fpath.write_text(json.dumps(record, indent=2))

    return medicion


def run_scenario(executor: MininetExecutor, topology: str, esc: str,
                 out_dir: Path, dry_run: bool) -> None:
    """Run a complete scenario"""
    escenario_cfg = SCENARIO["escenarios"][esc]
    tcp_senders = escenario_cfg["tcp_senders"]
    udp_sender = escenario_cfg["udp_sender"]

    print(f"\n{'='*70}")
    print(f"  F5-{esc.upper()}  |  Topology: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  {escenario_cfg['desc']}")
    print(f"  TCP senders: {len(tcp_senders)} -> {escenario_cfg['tcp_receptor']}")
    print(f"  UDP sender: {udp_sender} @ {UDP_RATE} -> {escenario_cfg['udp_receptor']}")
    print(f"  Cooldown: {COOLDOWN}s  |  Duration: {DURATION}s")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Output: {out_dir}")
    if dry_run:
        print("  MODE: DRY-RUN")
    print(f"{'='*70}\n")

    # Start servers
    puertos = start_servers(executor, escenario_cfg, dry_run)

    goodputs = []
    jitters = []
    retrans_totales = []
    reps_hechas = 0
    t0 = time.time()

    for rep in range(1, REPS_MAX + 1):
        if rep > 1 and not dry_run:
            print(f"  Cooling down {COOLDOWN}s...", end="", flush=True)
            for i in range(COOLDOWN, 0, -1):
                print(f"\r  Cooling down {i}s...   ", end="", flush=True)
                time.sleep(1)
            print()

        print(f"  Rep {rep:02d}/{REPS_MAX} measuring...", end="", flush=True)

        m = run_rep(executor, escenario_cfg, puertos, rep, topology, esc,
                   out_dir, dry_run)
        reps_hechas = rep

        if dry_run:
            break

        if not m["ok"]:
            print(f"\r  Rep {rep:02d}/{REPS_MAX} INVALID - skipped")
            continue

        goodputs.append(m["goodput_aggregate_mbps"])
        jitters.append(m["udp_jitter_ms"])
        retrans_total = sum(m.get("retransmits", {}).values())
        retrans_totales.append(retrans_total)

        lr_pct = (m["goodput_aggregate_mbps"] / LINE_RATE_MBPS) * 100
        print(f"\r  Rep {rep:02d}/{REPS_MAX} goodput={m['goodput_aggregate_mbps']:.1f} Mbps "
              f"({lr_pct:.1f}% LR) jitter={m['udp_jitter_ms']:.3f}ms "
              f"lost={m['udp_lost_pct']:.2f}% retrans={retrans_total}")

        if rep >= REPS_MIN:
            r_g = compute_rsd(goodputs)
            r_j = compute_rsd(jitters)
            if r_g is not None and r_j is not None:
                print(f"  RSD goodput={r_g:.2f}% jitter={r_j:.2f}% "
                      f"(target < {RSD_TARGET}%)")
                if r_g < RSD_TARGET and r_j < RSD_TARGET:
                    print(f"  Converged in {rep} reps")
                    break

    if not dry_run:
        stop_servers(executor, escenario_cfg, dry_run)

    if dry_run or not goodputs:
        return

    # Generate summary
    final_goodput = statistics.mean(goodputs)
    final_jitter = statistics.mean(jitters)
    final_retrans = statistics.mean(retrans_totales) if retrans_totales else 0

    summary = {
        "experiment": EXPERIMENT,
        "scenario": esc,
        "topology": topology,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "tcp_receptor": escenario_cfg["tcp_receptor"],
        "udp_receptor": escenario_cfg["udp_receptor"],
        "tcp_senders": tcp_senders,
        "udp_sender": udp_sender,
        "duration_s": DURATION,
        "cooldown_s": COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "udp_rate": UDP_RATE,
        "reps_valid": len(goodputs),
        "reps_launched": reps_hechas,
        "goodput_aggregate_mbps": {
            "mean": round(final_goodput, 2),
            "rsd_pct": round(compute_rsd(goodputs) or 0, 2),
            "line_rate_pct": round((final_goodput / LINE_RATE_MBPS) * 100, 2),
        },
        "udp_jitter_ms": {
            "mean": round(final_jitter, 4),
            "rsd_pct": round(compute_rsd(jitters) or 0, 2),
        },
        "tcp_retransmits_total": {
            "mean": round(final_retrans, 1),
            "rsd_pct": round(compute_rsd(retrans_totales) or 0, 2) if retrans_totales else None,
        },
        "duration_total_min": round((time.time() - t0) / 60, 1),
    }

    summary_path = out_dir / f"{topology}_{EXPERIMENT}{esc}_summary.json"
    save_summary(summary_path, summary)
    print(f"\n  Summary: {summary_path}")

    # Print summary table
    print(f"\n{'='*70}")
    print(f"  SUMMARY F5-{esc.upper()} - {topology.upper()}")
    print(f"  TCP senders: {len(tcp_senders)} | UDP: {udp_sender} @ {UDP_RATE}")
    print(f"  {'Goodput Mbps':>15} {'LR%':>8} {'Jitter ms':>12} {'Retrans':>10} {'Reps':>6}")
    print(f"  {'-'*15} {'-'*8} {'-'*12} {'-'*10} {'-'*6}")
    print(f"  {final_goodput:>15.2f} {summary['goodput_aggregate_mbps']['line_rate_pct']:>7.1f}% "
          f"{final_jitter:>12.4f} {final_retrans:>10.1f} {len(goodputs):>6}")
    print(f"{'='*70}\n")


def run_experiment(executor: MininetExecutor, topology: str,
                   escenarios: List[str], dry_run: bool) -> None:
    """Run complete F5 experiment"""
    out_dir = OUTPUT_BASE / topology / "fase5_incast"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  F5 INCAST  |  Topology: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  {SCENARIO['desc']}")
    print(f"  Scenarios: {escenarios}")
    print(f"  Cooldown: {COOLDOWN}s  |  Duration: {DURATION}s")
    print(f"  TCP: {TCP_CONGESTION}  |  UDP rate: {UDP_RATE}")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Output: {out_dir}")
    if dry_run:
        print("  MODE: DRY-RUN")
    print(f"{'='*70}\n")

    for esc in escenarios:
        run_scenario(executor, topology, esc, out_dir, dry_run)
        if esc != escenarios[-1] and not dry_run:
            time.sleep(RATE_PAUSE)


def main():
    parser = argparse.ArgumentParser(
        description="F5: Incast Testing with Mininet (RFC 8239 Section 6)"
    )
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl",
                        help="Topology: sl (spine-leaf) or ft (fat-tree)")
    parser.add_argument("--scenario", choices=["s1", "s2"], default=None,
                        help="Run only one scenario (default: both)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate execution without real tests")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip preflight verification")
    parser.add_argument("--cli", action="store_true",
                        help="Start Mininet CLI after configuration")
    parser.add_argument("--controller", type=str, default="172.17.0.2",
                        help="Controller IP (e.g., 172.17.0.2)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show verbose output including Mininet messages")
    args = parser.parse_args()

    # Set verbose mode
    set_verbose(args.verbose)

    global SCENARIO
    cfg = TOPOLOGIES[args.topology]
    SCENARIO = {
        "escenarios": cfg["escenarios"],
        "desc": cfg["scenario_desc"],
    }

    escenarios = [args.scenario] if args.scenario else ["s1", "s2"]

    if not args.verbose:
        setLogLevel('warning')
    else:
        setLogLevel('info')

    if not args.verbose:
        info("Creating network...\n")
    else:
        info(f"Creating topology {args.topology.upper()}\n")

    if args.dry_run:
        COOLDOWN = 0
        DURATION = 0
        RATE_PAUSE = 0

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

        # Preflight check
        if not args.dry_run and not args.skip_preflight:
            all_hosts = []
            for esc in escenarios:
                ecfg = SCENARIO["escenarios"][esc]
                all_hosts.extend(ecfg["tcp_senders"])
                all_hosts.append(ecfg["udp_sender"])
                all_hosts.append(ecfg["tcp_receptor"])
                all_hosts.append(ecfg["udp_receptor"])
            all_hosts = list(set(all_hosts))
            if not preflight(executor, all_hosts):
                warn("Preflight failed. Check iperf3 before continuing.\n")

        run_experiment(
            executor=executor,
            topology=args.topology,
            escenarios=escenarios,
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
