#!/usr/bin/env python3
"""
f1.py — F1 Line-Rate Testing con Mininet
Uso: sudo python3 f1_all_mininet.py --experiment all --topology sl
"""

import argparse
import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
# mininet functions
from mininet.net import Mininet
from mininet.cli import CLI
from mininet.log import setLogLevel, info, error, warn
from mininet.node import OVSSwitch, RemoteController
from mininet.link import TCLink

from topology import SpineLeaf, FatTree

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
        "desc": "Par único (carga baja) — baseline",
        "note": "F1-A1: par único H1→H4 — carga baja",
        "pairs": [
            {"id": "p1", "client": "H1", "server": "H4", "port": 5201},
        ]
    },
    "b1": {
        "desc": "2 pares cross-leaf (full-mesh parcial)",
        "note": "F1-B1: 2 pares cross-leaf simultáneos — full-mesh parcial",
        "pairs": [
            {"id": "p1", "client": "H2", "server": "H4", "port": 5201},
            {"id": "p2", "client": "H3", "server": "H7", "port": 5202},
        ]
    },
    "b2": {
        "desc": "4 pares cross-leaf CON balanceo manual",
        "note": "F1-B2: 4 flujos full-mesh, con balanceo manual",
        "pairs": [
            {"id": "p1", "client": "H1", "server": "H4", "port": 5201},
            {"id": "p2", "client": "H2", "server": "H7", "port": 5202},
            {"id": "p3", "client": "H3", "server": "H5", "port": 5203},
            {"id": "p4", "client": "H6", "server": "H8", "port": 5204},
        ]
    },
}

# Mininet Helpers
class MininetExecutor:
    """Gestiona la ejecución de comandos en Mininet"""

    def __init__(self, net: Mininet):
        self.net = net
        self.processes = []
        self.host_cache = {}

        # Crear cache de hosts para acceso rápido
        for host in net.hosts:
            self.host_cache[host.name] = host

    def get_host(self, host_key: str):
        """Obtiene el objeto host de Mininet"""
        # Convertir H1 -> h1
        mininet_name = host_key.lower()
        if mininet_name not in self.host_cache:
            # Intentar buscar el host
            try:
                host = self.net.get(mininet_name)
                self.host_cache[mininet_name] = host
                return host
            except:
                raise ValueError(f"Host {host_key} no encontrado en la red")
        return self.host_cache[mininet_name]

    def run_cmd(self, host_key: str, cmd: str, timeout: int = 90):
        """Ejecuta un comando en un host y retorna el resultado"""
        host = self.get_host(host_key)

        try:
            # Para comandos largos, usamos popen con timeout
            if 'iperf3' in cmd and '-t' in cmd:
                # iperf3 con timeout - ejecutar y esperar
                proc = host.popen(cmd, shell=True)
                try:
                    stdout, stderr = proc.communicate(timeout=timeout)
                    returncode = proc.returncode
                except Exception as e:
                    # Timeout o error
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    returncode = -1
                    stderr = str(e) if not stderr else stderr
            else:
                # Comandos simples
                result = host.cmd(cmd)
                stdout = result
                stderr = ""
                returncode = 0

            # Crear objeto similar a subprocess.CompletedProcess
            class ProcessResult:
                def __init__(self, stdout, stderr, returncode):
                    self.stdout = stdout
                    self.stderr = stderr
                    self.returncode = returncode

            return ProcessResult(stdout, stderr, returncode)

        except Exception as e:
            error(f"Error ejecutando comando en {host_key}: {e}\n")
            class ProcessResult:
                def __init__(self):
                    self.stdout = ""
                    self.stderr = str(e)
                    self.returncode = -1
            return ProcessResult()

    def run_bg(self, host_key: str, cmd: str):
        """Ejecuta un comando en background"""
        host = self.get_host(host_key)

        # Asegurar que SHELL está definida
        import os
        if 'SHELL' not in os.environ:
            os.environ['SHELL'] = '/bin/bash'

        try:
            proc = host.popen(cmd, shell=True)
            self.processes.append(proc)
            return proc
        except KeyError as e:
            # Si falla por SHELL, intentar con método alternativo
            warn(f"Error en popen: {e}. Intentando método alternativo...\n")
            # Usar el comando directamente con bash -c
            proc = host.popen(['/bin/bash', '-c', cmd])
            self.processes.append(proc)
            return proc

    def kill_iperf(self, host_key: str):
        """Mata procesos iperf3 en un host"""
        self.run_cmd(host_key, "pkill -9 iperf3 2>/dev/null; true", timeout=5)

    def kill_iperf_all(self, pairs: List[dict]):
        """Mata iperf3 en clientes Y servidores"""
        hosts = {p["client"] for p in pairs} | {p["server"] for p in pairs}
        for h in hosts:
            self.kill_iperf(h)

    def cleanup(self):
        """Limpia procesos en background"""
        for proc in self.processes:
            try:
                if proc.poll() is None:
                    proc.kill()
            except:
                pass
        self.processes = []

# Statistic functions
def inject_meta(data: dict, pair: dict, pkt_size: int, rep: int,
                topology: str, experiment: str, note: str) -> dict:
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
        "server_ip":     HOSTS[pair["server"]]["ip"],
        "pkt_size_b":    pkt_size,
        "rep":           rep,
        "duration_s":    DURATION,
        "cooldown_s":    COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rfc_reference": "RFC8239 §2 — Line-Rate Testing (Mininet)",
        "note":          note,
        "snd_cwnd_avg_bytes": cwnd_avg,
        "snd_cwnd_max_bytes": cwnd_max,
    }
    return data

def compute_rsd(values: List[float]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def extract_mbps(data: dict) -> Optional[float]:
    try:
        return data["end"]["sum_sent"]["bits_per_second"] / 1e6
    except (KeyError, TypeError):
        return None

# Experiment functions
def start_servers(executor: MininetExecutor, pairs: List[dict]) -> None:
    executor.kill_iperf_all(pairs)
    time.sleep(1)
    for p in pairs:
        executor.run_bg(p["server"], f"iperf3 -s -p {p['port']}")
    time.sleep(2)

def run_single_pair(executor: MininetExecutor, pair: dict, pkt_size: int,
                    rep: int, topology: str, experiment: str, note: str,
                    out_dir: Path, results: dict, errors: list,
                    dry_run: bool) -> None:
    fname = (
        f"{topology}_{experiment}_tcp"
        f"_pkt{pkt_size:04d}"
        f"_{pair['id']}"
        f"_rep{rep:02d}.json"
    )
    fpath = out_dir / fname
    srv_ip = HOSTS[pair["server"]]["ip"]

    if dry_run:
        print(f"    [DRY] {pair['id']}: iperf3 -c {srv_ip} -p {pair['port']}"
              f" -t {DURATION} -Z -C {TCP_CONGESTION} -l {pkt_size} -J  →  {fname}")
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

        # Verificar que la salida sea JSON válido
        try:
            data = json.loads(res.stdout)
        except json.JSONDecodeError as e:
            errors.append(f"{pair['id']}: JSON decode error: {e}")
            errors.append(f"Output snippet: {res.stdout[:200]}")
            results[pair["id"]] = None
            return

        data = inject_meta(data, pair, pkt_size, rep, topology, experiment, note)
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = extract_mbps(data)

    except Exception as e:
        errors.append(f"{pair['id']}: {e}")
        results[pair["id"]] = None
        executor.kill_iperf(pair["client"])

def run_rep(executor: MininetExecutor, pairs: List[dict], pkt_size: int,
            rep: int, topology: str, experiment: str, note: str,
            out_dir: Path, dry_run: bool) -> List[Optional[float]]:
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
        print(f"    ⚠  {err}")

    return [results.get(p["id"]) for p in pairs]

# CONFIGURACION DE RED

def configure_tcp(executor: MininetExecutor):
    """Configura TCP en todos los hosts"""
    for host_key in HOSTS.keys():
        executor.run_cmd(host_key, f"sysctl -w net.ipv4.tcp_congestion_control={TCP_CONGESTION}")
        executor.run_cmd(host_key, "sysctl -w net.ipv4.tcp_no_metrics_save=1")
        executor.run_cmd(host_key, "sysctl -w net.ipv4.tcp_slow_start_after_idle=0")

def create_network(topology_type: str, controller_ip: str = None):
    """
    Crea y retorna una red Mininet con la topología especificada
    """
    if topology_type.lower() == 'sl':
        topo = SpineLeaf()
    elif topology_type.lower() == 'ft':
        topo = FatTree()
    else:
        raise ValueError(f"Topología no soportada: {topology_type}")

    # Usar controlador remoto si se especifica, sino OVSSwitch sin controller
    if controller_ip:
        controller = RemoteController('odl', ip=controller_ip, port=6653)
        net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    else:
        # Usar switch OVS con controller por defecto (OpenFlow)
        net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch)

    return net

# Main
def run_experiment(executor: MininetExecutor, experiment: str, topology: str,
                   dry_run: bool) -> None:
    exp_config = EXPERIMENTS[experiment]
    pairs = exp_config["pairs"]
    note = exp_config["note"]
    desc = exp_config["desc"]

    out_dir = OUTPUT_BASE / topology / "fase1_linerate"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  F1-{experiment.upper()}  |  Topología: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Descripción: {desc}")
    print(f"  Cooldown:   {COOLDOWN}s  |  TCP: {TCP_CONGESTION}")
    print("  Pares:  " + "  ".join(f"{p['id']}:{p['client']}->{p['server']}" for p in pairs))
    print(f"  Salida: {out_dir}")
    print(f"  PKTs:   {PKT_SIZES}")
    print(f"  Reps:   {REPS_MIN}-{REPS_MAX}  (RSD < {RSD_TARGET}%)")
    if dry_run:
        print("  MODO:   DRY-RUN")
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
                print("  ✘ todos los pares fallaron", end="")

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
            print(f"\n  ⚠  RSD no convergió en {REPS_MAX} reps")

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
        print(f"  → {rep} reps | {final_mean:.2f} Mbps | RSD={rsd_str} | {lr_pct:.1f}% LR\n")

        if not dry_run:
            executor.kill_iperf_all(pairs)
            if pkt_size != PKT_SIZES[-1]:
                time.sleep(PKT_PAUSE)

    # Tabla resumen
    print(f"\n{'='*70}")
    print(f"  RESUMEN F1-{experiment.upper()} -- {topology.upper()}")
    print(f"  Cooldown: {COOLDOWN}s | TCP: {TCP_CONGESTION}")
    print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
    print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
    for pkt, s in summary.items():
        rsd_str = f"{s['rsd_pct']:.1f}" if s["rsd_pct"] is not None else "N/A"
        print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}"
              f"  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
    print(f"{'='*70}\n")

    summary_path = out_dir / f"{topology}_{experiment}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment":    experiment,
        "topology":      topology,
        "rfc_reference": "RFC8239 §2 (Mininet)",
        "desc":          desc,
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
    print(f"  Resumen: {summary_path}\n")

#  Preflight
def preflight(executor: MininetExecutor, experiment: str) -> bool:
    pairs = EXPERIMENTS[experiment]["pairs"]
    involved = sorted(
        {p["client"] for p in pairs} | {p["server"] for p in pairs}
    )
    print("-- Preflight check " + "-"*52)
    all_ok = True
    for name in involved:
        res = executor.run_cmd(name, "iperf3 --version 2>&1 | head -1", timeout=10)
        ok = res.returncode == 0
        ver = res.stdout.strip()[:55] if ok else res.stderr.strip()[:55]
        print(f"  {'OK' if ok else 'FAIL'} {name:4s}  {HOSTS[name]['ip']:15s}  {ver}")
        if not ok:
            all_ok = False
    print()
    return all_ok

# Entry
def main():
    parser = argparse.ArgumentParser(
        description="F1: Line-Rate Testing con Mininet"
    )
    parser.add_argument(
        "--experiment",
        choices=["a1", "b1", "b2", "all"],
        default="all",
        help="Experimento a ejecutar default all."
    )
    parser.add_argument(
        "--topology",
        choices=["sl", "ft"],
        default="sl",
        help="Topología: sl (spine-leaf) o ft (fat-tree)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simular ejecución sin hacer pruebas reales")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Saltar verificación de preflight")
    parser.add_argument("--cli", action="store_true",
                        help="Iniciar CLI de Mininet después de la configuración")
    parser.add_argument("--controller", type=str, default="172.17.0.2",
                        help="IP del controlador remoto (ej: 172.17.0.2)")
    args = parser.parse_args()

    # Configurar logging de Mininet
    setLogLevel('info')

    if args.experiment == "all":
        experiments = ["a1", "b1", "b2"]
    else:
        experiments = [args.experiment]

    # Crear la red
    info(f"*** Creando topología {args.topology.upper()}\n")
    net = create_network(args.topology, args.controller)

    try:
        info("*** Iniciando red\n")
        net.start()

        # Crear executor
        executor = MininetExecutor(net)

        # Configurar TCP
        info("*** Configurando TCP\n")
        configure_tcp(executor)

        # Verificar que iperf3 está disponible
        info("*** Verificando iperf3\n")
        # Probar en un host
        test_host = "H1"
        test_cmd = executor.run_cmd(test_host, "which iperf3 2>/dev/null || echo 'not found'")

        if "not found" in test_cmd.stdout or test_cmd.returncode != 0:
            warn("*** iperf3 no encontrado. Instalando en todos los hosts...\n")
            # Intentar instalar iperf3 en los hosts
            for host in net.hosts:
                # Usar apt-get si está disponible (asume Debian/Ubuntu)
                result = host.cmd("apt-get update -qq 2>/dev/null && apt-get install -y iperf3 2>/dev/null || true")
                # Si falla apt-get, intentar con yum (CentOS/RHEL)
                if "iperf3" not in host.cmd("which iperf3 2>/dev/null || echo 'not found'"):
                    host.cmd("yum install -y iperf3 2>/dev/null || true")

        # Ejecutar experimentos
        for exp in experiments:
            if not args.dry_run and not args.skip_preflight:
                if not preflight(executor, exp):
                    warn(f"  FAIL: Preflight falló para {exp}. Verifica iperf3 antes de continuar.\n")
                    continue

            run_experiment(
                executor=executor,
                experiment=exp,
                topology=args.topology,
                dry_run=args.dry_run
            )

        # Si se solicita, iniciar CLI
        if args.cli:
            info("*** Iniciando CLI (salir con 'exit')\n")
            CLI(net)

    except KeyboardInterrupt:
        info("\n*** Interrupción por usuario\n")
    except Exception as e:
        error(f"*** Error: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        info("*** Limpiando...\n")
        # Limpiar procesos
        if 'executor' in locals():
            executor.cleanup()
        # Detener red
        net.stop()
        info("*** Hecho\n")

if __name__ == "__main__":
    main()
