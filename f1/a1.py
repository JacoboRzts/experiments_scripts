#!/usr/bin/env python3
"""
F1A1 Mininet
USE: sudo python3 a1.py --topology [ sl | ft ] --dry-run
"""

import json
import statistics
import argparse
import time

from mininet_common import (
    create_network, iperf_server_start, iperf_server_kill,
    iperf_client_run, inject_meta, compute_rsd,
    DURATION, PKT_SIZES, RSD_TARGET, REPS_MIN, REPS_MAX,
    COOLDOWN, PKT_PAUSE, OUTPUT_BASE, preflight, stop_network
)

EXPERIMENT = "f1a1"
PAIR = {
    "id": "p1",
    "client": "h1",
    "server": "h5",
    "server_ip": "10.0.2.1",
    "port": 5201,
}

def run_experiment(topology, dry_run):
    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not dry_run:
        net = create_network(topology)
        hosts = {h.name: h for h in net.hosts}
    else:
        net = None
        hosts = None

    PAIR["server_ip"] = hosts[PAIR["server"]].IP()

    print(f"\n{'='*65}")
    print(f"  F1-A1 (Mininet) | Topología: {topology.upper()}")
    print(f"  Par: {PAIR['client']} ({PAIR['client']}) -> {PAIR['server']} ({PAIR['server_ip']})")
    print(f"  Salida: {out_dir}")
    print(f"  PKTs: {PKT_SIZES}")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    if dry_run:
        print("  MODO: DRY-RUN")
    print(f"{'='*65}\n")

    summary = {}

    try:
        for pkt_size in PKT_SIZES:
            print(f"-- PKT {pkt_size:>4d} B " + "-"*45)
            throughputs = []

            if not dry_run:
                # Preparar servidor
                iperf_server_kill(hosts[PAIR["server"]])
                time.sleep(1)
                iperf_server_start(hosts[PAIR["server"]], PAIR["port"])

            rep = 1
            converged = False
            while rep <= REPS_MAX:
                print(f"  Rep {rep:02d}/{REPS_MAX}  ", end="", flush=True)

                if dry_run:
                    print(f"  [DRY] iperf3 -c {PAIR['server_ip']} -p {PAIR['port']} -t {DURATION} -Z -l {pkt_size}")
                    mbps = None
                else:
                    data, mbps = iperf_client_run(
                        hosts[PAIR["client"]], PAIR["server_ip"],
                        PAIR["port"], pkt_size, DURATION
                    )
                    if data:
                        data = inject_meta(data, PAIR["id"], pkt_size, rep, topology, EXPERIMENT)
                        fname = f"{topology}_{EXPERIMENT}_pkt{pkt_size:04d}_rep{rep:02d}.json"
                        (out_dir / fname).write_text(json.dumps(data, indent=2))

                if mbps is not None:
                    throughputs.append(mbps)
                    print(f"  {mbps:8.2f} Mbps", end="")
                else:
                    print("  ✘ fallo", end="")

                if rep >= REPS_MIN and len(throughputs) >= REPS_MIN:
                    rsd = compute_rsd(throughputs)
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

            final_mean = statistics.mean(throughputs) if throughputs else 0.0
            final_rsd = compute_rsd(throughputs)
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
                iperf_server_kill(hosts[PAIR["server"]])
                if pkt_size != PKT_SIZES[-1]:
                    time.sleep(PKT_PAUSE)

        # Tabla resumen
        print(f"\n{'='*65}")
        print(f"  RESUMEN F1-A1 -- {topology.upper()} (Mininet)")
        print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
        print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
        for pkt, s in summary.items():
            rsd_str = f"{s['rsd_pct']:.1f}" if s['rsd_pct'] else "N/A"
            print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
        print(f"{'='*65}\n")

        summary_path = out_dir / f"{topology}_{EXPERIMENT}_summary.json"
        summary_path.write_text(json.dumps({
            "experiment": EXPERIMENT,
            "topology": topology,
            "pair": PAIR,
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
