#!/usr/bin/env python3
"""
f1.py — F1 Line-Rate
"""

import argparse
import json
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Importar desde topology.py
from topology import build_network

# Configuration
DURATION = 30
COOLDOWN = 5
PKT_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 2
REPS_MAX = 2
PKT_SIZES = [64, 128, 256, 512, 1024, 1518]
TCP_CONGESTION = "cubic"

OUTPUT_BASE = Path.home() / "results"

#  CONFIGURACIÓN POR TOPOLOGÍA — CORREGIDA
CONFIGURATION = {
    "sl": {
        "experiments": {
            "a1": [
                {
                    "id": "p1",
                    "server": "h4",
                    "client": "h1",
                    "port": 5201
                },
            ],
            "b1": [
                {
                    "id": "p1",
                    "server": "h4",
                    "client": "h2",
                    "port": 5201
                },
                {
                    "id": "p2",
                    "server": "h7",
                    "client": "h3",
                    "port": 5202,
                }
            ],
            "b2": [
                {
                    "id": "p1",
                    "client": "h1",
                    "server": "h4",
                    "port": 5201
                },
                {
                    "id": "p2",
                    "client": "h2",
                    "server": "h7",
                    "port": 5202
                },
                {
                    "id": "p3",
                    "client": "h3",
                    "server": "h5",
                    "port": 5203
                },
                {
                    "id": "p4",
                    "client": "h6",
                    "server": "h8",
                    "port": 5204
                },
            ],
        }
    },
    "j3c": {
        "experiments": {
            "a1": [
                {
                    "id": "p1",
                    "server": "h5",
                    "client": "h1",
                    "port": 5201
                },
            ],
            "b1": [
                {
                    "id": "p1",
                    "server": "h4",
                    "client": "h2",
                    "port": 5201
                },
                {
                    "id": "p2",
                    "server": "h7",
                    "client": "h3",
                    "port": 5202,
                }
            ],
            "b2": [
                {
                    "id": "p1",
                    "client": "h1",
                    "server": "h5",
                    "port": 5201
                },
                {
                    "id": "p2",
                    "client": "h2",
                    "server": "h6",
                    "port": 5202
                },
                {
                    "id": "p3",
                    "client": "h3",
                    "server": "h7",
                    "port": 5203
                },
                {
                    "id": "p4",
                    "client": "h4",
                    "server": "h8",
                    "port": 5204
                },
            ],
        }
    }
}

# COLOR FUNCTIONS
class C:
    OK = "\033[32m"     # Green
    WRN = "\033[93m"    # Yellow
    ERR = "\033[91m"    # Red
    INFO = "\033[0;34m" # Blue
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"

def ok(msg, end="\n"):    print(f"{C.OK}  {msg} {C.END}", end=end)
def info(msg, end="\n", start=" "):  print(f"{start} {C.INFO}INFO{C.END} {msg}", end=end)
def warn(msg, end="\n", start=" "):  print(f"{start} {C.WRN}WARNING{C.END} {msg}", end=end)
def error(msg, end="\n", start=" "): print(f"{start} {C.ERR}ERROR{C.END} {msg}", end=end)

def check_connectivity(client, server_ip, port, timeout=3):
    cmd = f"timeout {timeout} nc -zv {server_ip} {port} 2>&1"
    result = client.cmd(cmd)
    if "succeeded" not in result and "Connected" not in result:
        return False
    return True

def is_iperf3_active(host) -> bool:
    result = host.cmd("pgrep -f iperf3 2>/dev/null")
    return bool(result.strip())

def kill_iperf(host):
    host.cmd("pkill -9 iperf3")
    time.sleep(1)

def kill_all_iperf(net):
    hosts = net.hosts
    for host in hosts:
        if is_iperf3_active(host):
            kill_iperf(host)

def start_servers(net, pairs):
    info("Starting servers")
    for pair in pairs:
        client = net.get(pair['client'])
        server = net.get(pair["server"])
        port = pair["port"]

        cmd = f"iperf3 -s -p {port} -D"
        server.cmd(cmd)
        time.sleep(1)

        # Check if the server start.
        check = check_connectivity(client, server.IP(), port)
        if check:
            ok(f"Server {server} started on port {port}")
        else:
            error(f"Can't start server on {server} with port {port} using command {cmd}")

    time.sleep(1)
    print('')

# stats
def inject_meta(data, pair, pkt_size, rep, topology, experiment):
    # Extraer cwnd de los intervalos
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

    data["_meta"] = {
        "experiment":    experiment,
        "topology":      topology,
        "pair_id":       pair["id"],
        "client_host":   pair["client"],
        "server_host":   pair["server"],
        "pkt_size_b":    pkt_size,
        "rep":           rep,
        "duration_s":    DURATION,
        "cooldown_s":    COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rfc_reference": "RFC8239 §2 — Line-Rate Testing",
        "snd_cwnd_avg_bytes": cwnd_avg,
        "snd_cwnd_max_bytes": cwnd_max,
    }
    return data

def compute_rsd(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def extract_mbps(data):
    try:
        return data["end"]["sum_sent"]["bits_per_second"] / 1e6
    except (KeyError, TypeError):
        return None

# Preflight
def preflight(hosts):
    all_ok = True
    hosts_with_iperf = 0
    for host in hosts:
        output = host.cmd("iperf3 --version 2>&1")
        host_ok = bool(output.strip())
        if not host_ok:
            error(f"{host.name} has not iperf3 installed.")
            all_ok = False
        else:
            hosts_with_iperf += 1
    info(f"iperf3 installed on {hosts_with_iperf} hosts.")
    return all_ok

# LÓGICA DE EXPERIMENTO
def run_single_pair(net, pair, pkt_size, rep, topology, experiment, out_dir, results, errors, dry_run):
    fname = (f"{topology}_{experiment}_tcp_pkt{pkt_size:04d}_{pair['id']}_rep{rep:02d}.json")
    fpath = out_dir / fname
    client = net.get(pair['client'])
    server = net.get(pair['server'])
    server_ip = server.IP()
    cmd = (f"iperf3 -c {server_ip} -p {pair['port']} -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J")
    pid = pair['id']
    results[pid] = None

    if dry_run:
        print(f"{cmd} -> {fname}")
        return
    try:
        output, err, exitcode = client.pexec(cmd)
        if exitcode != 0:
            error(f"Exitcode {exitcode}, {err.strip()}")
            if check_connectivity(client, server.IP(), pair['port']):
                ok("Server running")
            else:
                error("Server is not running")
        if not output.strip():
            error(f"{pid}: iperf3 empty output.")
            return
        try:
            data = json.loads(output)
        except json.JSONDecodeError as e:
            debug_path = out_dir / f"RAW_{fname}.txt"
            debug_path.write_text(output)
            error(f"{pid}: JSON inválido: {e} (raw guardado en {debug_path.name})")
            return
        data = inject_meta(data, pair, pkt_size, rep, topology, experiment)
        data["_meta"]["server_ip"] = server_ip
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = extract_mbps(data)
    except Exception as e:
        error(f"{pid}: Excepción inesperada: {type(e).__name__}: {e}")
        kill_iperf(client)

def run_rep(net, pairs, pkt_size, rep, topology, experiment, out_dir, dry_run):
    results = {}
    errors = []

    threads = [
        threading.Thread(
            target=run_single_pair,
            args=(net, p, pkt_size, rep, topology, experiment,
                  out_dir, results, errors, dry_run),
        )
        for p in pairs
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join(timeout=DURATION + 35)

    # Mostrar errores (igual que antes)
    for err in errors:
        error(f"{err.split(':')[0] if ':' in err else 'UNKNOWN'}")
        lines = err.split('\n')
        for line in lines:
            if line.strip():
                print(line)

    return [results.get(p["id"]) for p in pairs]

def run_experiment(net, experiment, topology, dry_run):
    pairs = CONFIGURATION[topology]["experiments"][experiment]

    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    print('─'*64)
    print(f"  Experiment:  {experiment.upper()}")
    print(f"  Datetime:    {datetime.now():%Y-%m-%d %H:%M:%S}")
    print( "  Pairs:       " + "  ".join(f"{p['id']}:{p['client']}->{p['server']}" for p in pairs))
    if dry_run:
        print( "  Modo:        Dry-Run")
    print('─'*64)
    print()

    summary = {}
    for pkt_size in PKT_SIZES:
        throughputs_per_rep = []
        if not dry_run:
            start_servers(net, pairs)
        print(f"─ PKT {pkt_size:>4d} B " + "─"*50)
        rep = 1
        converged = False
        while rep <= REPS_MAX:
            print(f"  {rep:02d}/{REPS_MAX:02d}", end="")
            mbps_list = run_rep(net, pairs, pkt_size, rep, topology, experiment, out_dir, dry_run)
            valid = [v for v in mbps_list if v is not None]
            if valid:
                rep_mean = statistics.mean(valid)
                throughputs_per_rep.append(rep_mean)
                ok(f"{rep_mean:8.2f}Mbps [{len(valid)}/{len(pairs)} ok]", end="")
            else:
                error("All pairs faild.", end="")
            if rep >= REPS_MIN and len(throughputs_per_rep) >= REPS_MIN:
                rsd = compute_rsd(throughputs_per_rep)
                if rsd is not None:
                    print(f" RSD={rsd:.1f}%", end="")
                    if rsd <= RSD_TARGET:
                        ok("converge")
                        converged = True
                        break
            print()
            if rep < REPS_MAX:
                time.sleep(COOLDOWN)
            rep += 1
        if not converged:
            warn(f"RSD does not converge in {REPS_MAX} iterations.")

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
        print(f"─ {rep} reps | {final_mean:.2f} Mbps | RSD={rsd_str} | {lr_pct:.1f}% LR " + "─"*18)
        print()

        if not dry_run:
            kill_all_iperf(net)
            if pkt_size != PKT_SIZES[-1]:
                time.sleep(PKT_PAUSE)

    # Tabla resumen
    print(f"\n{'─'*70}")
    print(f"  SUMMARY F1 - {experiment.upper()} - {topology.upper()}")
    print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
    for pkt, s in summary.items():
        rsd_str = f"{s['rsd_pct']:.1f}" if s["rsd_pct"] is not None else "N/A"
        print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}"
              f"  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
    print(f"{'─'*70}\n")

    summary_path = out_dir / f"{topology}_{experiment}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment":    experiment,
        "topology":      topology,
        "rfc_reference": "RFC8239 §2",
        "cooldown_s":    COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "pairs": [{"id": p["id"], "client": p["client"],
                   "server": p["server"]} for p in pairs],
        "pkt_sizes":     PKT_SIZES,
        "rsd_target":    RSD_TARGET,
        "reps_min":      REPS_MIN,
        "reps_max":      REPS_MAX,
        "duration_s":    DURATION,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "results":       {str(k): v for k, v in summary.items()},
    }, indent=2))
    ok(f"  Summary: {summary_path}\n")

def main():
    parser = argparse.ArgumentParser(description="F1: Line-Rate Testing con API Mininet")
    parser.add_argument("-e", "--experiment", choices=["a1", "b1", "b2", "all"], default="all", help="Experimento a ejecutar (all = todos secuencialmente)")
    parser.add_argument("-t", "--topology", choices=["sl", "j3c"], default="sl", help="Topología: sl (spine-leaf) o j3c (jerárquica 3 capas)")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-s", "--skip-preflight", action="store_true")
    parser.add_argument("-c", "--controller-ip", default="172.17.0.2", help="IP del controlador SDN (por defecto: 172.17.0.2)")
    args = parser.parse_args()

    print('─'*64)
    print("  Fase 1 - Line-Rate")
    print('─'*64)

    net = build_network(topology=args.topology, controller_ip=args.controller_ip)
    try:
        # start network
        net.start()

        # set variables
        hosts = net.hosts
        experiments = ["a1", "b1", "b2"] if args.experiment == "all" else [args.experiment]
        topology = args.topology

        info(f"Experiments: {experiments}")
        info(f"Topology: {topology}")
        info(f"Package sizes: {PKT_SIZES}")
        if not args.skip_preflight and not args.dry_run:
            if not preflight(hosts):
                error("Check all the tools before start again.")
                return

        for experiment in experiments:
            run_experiment(net, experiment, topology, args.dry_run)

    except Exception as e:
        error(str(e))
    finally:
        net.stop()

if __name__ == "__main__":
    main()
