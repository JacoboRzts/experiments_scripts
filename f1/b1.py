#!/usr/bin/env python3
"""
F1B1 Mininet
USE: sudo python3 b1.py --topology [ sl | ft ] --dry-run
"""

import argparse
import json
import statistics
import threading
import time

from mininet_common import (
    create_network, iperf_server_start, iperf_server_kill,
    iperf_client_run, inject_meta, compute_rsd,
    DURATION, PKT_SIZES, RSD_TARGET, REPS_MIN, REPS_MAX,
    COOLDOWN, PKT_PAUSE, OUTPUT_BASE, preflight, stop_network
)

EXPERIMENT = "f1b1"

PAIRS = [
    {"id": "p1", "client": "h1", "server": "h5", "server_ip": "10.0.2.1", "port": 5201},
    {"id": "p2", "client": "h4", "server": "h8", "server_ip": "10.0.3.1", "port": 5202},
]

def run_single_pair(pair, pkt_size, rep, topology, out_dir, results, errors, dry_run, hosts):
    fname = f"{topology}_{EXPERIMENT}_pkt{pkt_size:04d}_{pair['id']}_rep{rep:02d}.json"
    fpath = out_dir / fname
    if dry_run:
        print(f"    [DRY] {pair['id']}: iperf3 -c {pair['server_ip']} -p {pair['port']} -t {DURATION} -Z -l {pkt_size}")
        results[pair["id"]] = None
        return
    client = hosts[pair["client"]]
    data, mbps = iperf_client_run(client, pair["server_ip"], pair["port"], pkt_size, DURATION)
    if data:
        data = inject_meta(data, pair["id"], pkt_size, rep, topology, EXPERIMENT)
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = mbps
    else:
        errors.append(f"{pair['id']}: fallo")
        results[pair["id"]] = None

def run_rep(pkt_size, rep, topology, out_dir, dry_run, hosts):
    results = {}
    errors = []
    threads = [threading.Thread(target=run_single_pair,
                                args=(p, pkt_size, rep, topology, out_dir,
                                      results, errors, dry_run, hosts))
               for p in PAIRS]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=DURATION + 20)
    for err in errors:
        print(f"    ⚠  {err}")
    return [results.get(p["id"]) for p in PAIRS]

def run_experiment(topology, dry_run):
    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not dry_run:
        net = create_network(topology)
        hosts = {h.name: h for h in net.hosts}
    else:
        net = None
        hosts = None

    for pair in PAIRS:
        pair["server_ip"] = hosts[pair["server"]].IP()

    print(f"\n{'='*65}")
    print(f"  F1-B1 (Mininet) | Topología: {topology.upper()}")
    print(f"  Pares: " + "  ".join(f"{p['id']}:{p['client']}->{p['server']}" for p in PAIRS))
    print(f"  Salida: {out_dir}")
    print(f"  PKTs: {PKT_SIZES}")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    if dry_run:
        print("  MODO DRY-RUN")
    print(f"{'='*65}\n")


    summary = {}
    try:
        for pkt_size in PKT_SIZES:
            print(f"-- PKT {pkt_size:>4d} B " + "-"*45)
            throughputs_per_rep = []

            if not dry_run:
                for p in PAIRS:
                    iperf_server_kill(hosts[p["server"]])
                time.sleep(1)
                for p in PAIRS:
                    iperf_server_start(hosts[p["server"]], p["port"])

            rep = 1
            converged = False
            while rep <= REPS_MAX:
                print(f"  Rep {rep:02d}/{REPS_MAX}  ", end="", flush=True)
                mbps_list = run_rep(pkt_size, rep, topology, out_dir, dry_run, hosts)
                valid = [v for v in mbps_list if v is not None]
                if valid:
                    rep_mean = statistics.mean(valid)
                    throughputs_per_rep.append(rep_mean)
                    print(f"  {rep_mean:8.2f} Mbps  [{len(valid)}/{len(PAIRS)} ok]", end="")
                else:
                    print("  ✘ todos fallaron", end="")

                if rep >= REPS_MIN and len(throughputs_per_rep) >= REPS_MIN:
                    rsd = compute_rsd(throughputs_per_rep)
                    if rsd is not None:
                        print(f"  RSD={rsd:.1f}%", end="")
                        if rsd < RSD_TARGET:
                            print("  ✔ converge")
                            converged = True
                            break
                print()
                if rep < REPS_MAX:
                    time.sleep(COOLDOWN)
                rep += 1

            if not converged:
                print(f"\n  ⚠  No convergió en {REPS_MAX} reps")

            final_mean = statistics.mean(throughputs_per_rep) if throughputs_per_rep else 0.0
            final_rsd = compute_rsd(throughputs_per_rep)
            lr_pct = (final_mean / 1000.0) * 100
            summary[pkt_size] = {
                "reps": rep,
                "mean_mbps": round(final_mean, 2),
                "rsd_pct": round(final_rsd, 2) if final_rsd else None,
                "lr_pct": round(lr_pct, 1),
            }
            rsd_str = f"{final_rsd:.1f}%" if final_rsd else "N/A"
            print(f"  → {rep} reps | {final_mean:.2f} Mbps | RSD={rsd_str} | {lr_pct:.1f}% LR\n")

            if not dry_run:
                for p in PAIRS:
                    iperf_server_kill(hosts[p["server"]])
                if pkt_size != PKT_SIZES[-1]:
                    time.sleep(PKT_PAUSE)

        print(f"\n{'='*65}")
        print(f"  RESUMEN F1-B1 -- {topology.upper()} (Mininet)")
        print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
        for pkt, s in summary.items():
            rsd_str = f"{s['rsd_pct']:.1f}" if s['rsd_pct'] else "N/A"
            print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
        print(f"{'='*65}\n")

        summary_path = out_dir / f"{topology}_{EXPERIMENT}_summary.json"
        summary_path.write_text(json.dumps({
            "experiment": EXPERIMENT,
            "topology": topology,
            "pairs": PAIRS,
            "pkt_sizes": PKT_SIZES,
            "results": {str(k): v for k, v in summary.items()},
        }, indent=2))
        print(f"  Resumen: {summary_path}\n")
    finally:
        if net:
            stop_network(net)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not preflight():
        return
    run_experiment(args.topology, args.dry_run)

if __name__ == "__main__":
    main()
