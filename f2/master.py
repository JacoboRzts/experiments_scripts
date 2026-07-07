#!/usr/bin/env python3
"""
run_f2_mininet.py - Orchestrate F2 experiments (S1+S2) inside Mininet.
Run from the host system (inside container) without sudo.
"""

import argparse
import sys
import time

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI

from topology import SpineLeaf, FatTree
from f2.executor import make_executors
import f2.s1 as s1
import f2.s2 as s2

CONTROLLER_IP = "172.17.0.2"
CONTROLLER_PORT = 6653
PAUSE_BETWEEN_SCENARIOS = 30

EXPERIMENTS = [
    {"id": "s1", "func": s1.run_scenario_s1, "desc": "H1+H4 -> H7 (min congestion)"},
    {"id": "s2", "func": s2.run_scenario_s2, "desc": "H1+H2+H4+H5 -> H7 (max congestion)"},
]

def preflight(executors):
    """Check iperf3 and ping availability on required hosts."""
    print("-- Preflight checks")
    all_ok = True
    required_hosts = ["H1", "H2", "H4", "H5", "H7"]
    for name in required_hosts:
        if name not in executors:
            continue
        ex = executors[name]
        res = ex.run("which iperf3 2>/dev/null", timeout=5)
        ok = (res.returncode == 0 and res.stdout.strip() != "")
        print(f"  {'OK' if ok else 'FAIL'} {name}  iperf3  {'found' if ok else 'missing'}")
        if not ok:
            all_ok = False
        # ping is usually available, but check anyway
        res = ex.run("ping -c 1 127.0.0.1 >/dev/null 2>&1 && echo ok", timeout=5)
        ok_ping = (res.returncode == 0 and 'ok' in res.stdout)
        print(f"  {'OK' if ok_ping else 'FAIL'} {name}  ping")
        if not ok_ping:
            all_ok = False
    return all_ok

def main():
    parser = argparse.ArgumentParser(description="F2 Experiments inside Mininet")
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl")
    parser.add_argument("--protocol", choices=["udp", "tcp", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--only", nargs="+", choices=["s1", "s2"], help="Run only these scenarios")
    args = parser.parse_args()

    protocols = ["udp", "tcp"] if args.protocol == "both" else [args.protocol]

    if args.topology == "sl":
        topo = SpineLeaf()
    else:
        topo = FatTree()

    print("Creating Mininet network...")
    controller = RemoteController('odl', ip=CONTROLLER_IP, port=CONTROLLER_PORT)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    net.start()

    # Map Mininet hosts to scenario host names
    # Topology: leaf1: h1,h2,h3; leaf2: h4,h5,h6; leaf3: h7,h8,h9
    try:
        host_map = {
            "H1": net.get('h1'),
            "H2": net.get('h2'),
            "H4": net.get('h4'),
            "H5": net.get('h5'),
            "H7": net.get('h7'),
        }
    except Exception as e:
        print(f"Error getting hosts: {e}")
        net.stop()
        sys.exit(1)

    executors = make_executors(net, host_map)

    if not args.dry_run and not args.skip_preflight:
        if not preflight(executors):
            print("Preflight failed. Please install iperf3 on all hosts and ensure ping works.")
            net.stop()
            sys.exit(1)

    # Select scenarios
    scenarios = EXPERIMENTS
    if args.only:
        scenarios = [e for e in EXPERIMENTS if e["id"] in args.only]

    print(f"\n{'='*65}")
    print(f"  F2 MASTER (Mininet)  |  {args.topology.upper()}")
    print(f"  Sequence: {' -> '.join(e['id'].upper() for e in scenarios)}")
    print(f"  Protocol: {args.protocol.upper()}")
    if args.dry_run:
        print("  DRY-RUN mode")
    print(f"{'='*65}")

    results = {}
    total_start = time.time()

    for i, exp in enumerate(scenarios):
        print(f"\n{'─'*65}")
        print(f"  Running {exp['id'].upper()} - {exp['desc']}")
        print(f"{'-'*65}")
        exp["func"](executors, args.topology, protocols, args.dry_run)
        results[exp["id"]] = True

        if i < len(scenarios)-1 and not args.dry_run:
            print(f"\n  Pause {PAUSE_BETWEEN_SCENARIOS}s before next scenario...")
            time.sleep(PAUSE_BETWEEN_SCENARIOS)

    total_elapsed = time.time() - total_start
    total_mins = int(total_elapsed // 60)
    total_secs = int(total_elapsed % 60)

    print(f"\n{'='*65}")
    print(f"  F2 MASTER FINISHED  |  {args.topology.upper()}")
    print(f"  Total time: {total_mins}m {total_secs}s")
    print(f"{'='*65}\n")

    net.stop()


if __name__ == "__main__":
    main()
