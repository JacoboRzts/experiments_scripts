#!/usr/bin/env python3
"""
F5 Mininet
"""

import argparse
import json
import os
import statistics
import sys
import threading
import time
from datetime import datetime
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from topology import SpineLeaf, FatTree

# ============================================================
# CONFIG
# ============================================================
CONFIG = {
    "hosts": {
        "H1": "h1",
        "H2": "h2",
        "H4": "h4",
        "H5": "h5",
        "H6": "h6",
        "H7": "h7",
    },
    "ips": {
        "h1": "10.0.1.1",
        "h2": "10.0.1.2",
        "h4": "10.0.1.4",
        "h5": "10.0.2.1",
        "h6": "10.0.2.2",
        "h7": "10.0.2.3",
    },
    # Spineleaf
    # "ips": {
    #     "h1": "10.0.1.1",
    #     "h2": "10.0.1.2",
    #     "h4": "10.0.2.1",
    #     "h5": "10.0.2.2",
    #     "h6": "10.0.2.3",
    #     "h7": "10.0.3.1",
    # },
    "receptor": "H7",
    "escenarios": {
        "s1": {"tcp_senders": ["H1", "H4"], "udp_sender": "H2"},
        "s2": {"tcp_senders": ["H1", "H4", "H5", "H6"], "udp_sender": "H2"},
    },
    "duracion_s": 30,
    "enfriamiento_s": 5,
    "reps_min": 15,
    "reps_max": 30,
    "rsd_objetivo_pct": 10.0,
    "tcp_puerto_base": 5201,
    "udp_puerto": 5400,
    "udp_tasa": "100M",
    "udp_payload_bytes": 1470,
    "line_rate_mbps": 1000,
    "dir_resultados": {
        "sl":  os.path.expanduser("~/experimentos/sl/fase5_incast"),
        "ft":  os.path.expanduser("~/experimentos/ft/fase5_incast"),
        "j3c": os.path.expanduser("~/experimentos/j3c/fase5_incast"),
    },
    "log_file": os.path.expanduser("~/fase5/f5_log.txt"),
}

net = None

# ============================================================
# Utilidades
# ============================================================

def log(msg, dry=False):
    pref = "[DRY] " if dry else ""
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {pref}{msg}"
    print(linea, flush=True)
    try:
        os.makedirs(os.path.dirname(CONFIG["log_file"]), exist_ok=True)
        with open(CONFIG["log_file"], "a") as f:
            f.write(linea + "\n")
    except OSError:
        pass


def rsd_pct(valores):
    if len(valores) < 2:
        return 100.0
    media = statistics.mean(valores)
    if media == 0:
        return 100.0
    return (statistics.stdev(valores) / media) * 100.0


def get_host(host_name):
    host_id = CONFIG["hosts"][host_name]
    return net.get(host_id)


# ============================================================
# Preflight
# ============================================================

def preflight(escenario_cfg, dry=False):
    log("== PREFLIGHT ==")
    ok = True
    involucrados = set(escenario_cfg["tcp_senders"]) | {escenario_cfg["udp_sender"], CONFIG["receptor"]}
    
    for h in sorted(involucrados):
        host_id = CONFIG["hosts"][h]
        if dry:
            log(f"  ✓ {h} ({host_id}) verificable")
            continue
        
        try:
            host = net.get(host_id)
            
            # Verificar iperf3
            result = host.cmd("which iperf3")
            if not result.strip():
                log(f"  ✗ {h}: iperf3 no encontrado")
                ok = False
                continue
            
            version = host.cmd("iperf3 --version | head -1").strip()
            log(f"  ✓ {h}: {version}")
            
            # Verificar conectividad al receptor
            rx_ip = CONFIG["ips"][CONFIG["hosts"][CONFIG["receptor"]]]
            ping_result = host.cmd(f"ping -c 1 {rx_ip} -W 2")
            if "1 received" not in ping_result:
                log(f"  ✗ {h}: No hay conectividad con {CONFIG['receptor']} ({rx_ip})")
                ok = False
            else:
                log(f"  ✓ {h}: Conectividad con {CONFIG['receptor']}")
                
        except Exception as e:
            log(f"  ✗ {h}: host no disponible - {e}")
            ok = False
    
    return ok


# ============================================================
# Núcleo del experimento
# ============================================================

def lanzar_servidores(escenario_cfg, dry=False):
    """Lanza servidores iperf3 en el receptor."""
    rx = get_host(CONFIG["receptor"])
    
    if not dry:
        # Matar procesos anteriores
        rx.cmd("pkill -9 iperf3 2>/dev/null; sleep 2")
        
        puertos = [CONFIG["tcp_puerto_base"] + i for i in range(len(escenario_cfg["tcp_senders"]))]
        
        # Lanzar servidores TCP
        for p in puertos:
            cmd = f"iperf3 -s -p {p} -D"
            result = rx.cmd(cmd)
            log(f"  Servidor TCP puerto {p}: {result if result else 'OK'}")
        
        # Lanzar servidor UDP
        cmd = f"iperf3 -s -p {CONFIG['udp_puerto']} -D"
        result = rx.cmd(cmd)
        log(f"  Servidor UDP puerto {CONFIG['udp_puerto']}: {result if result else 'OK'}")
        
        # Verificar que los servidores están corriendo
        time.sleep(2)
        ps_result = rx.cmd("ps aux | grep iperf3 | grep -v grep")
        log(f"  Servidores activos:\n{ps_result}")
    
    return [CONFIG["tcp_puerto_base"] + i for i in range(len(escenario_cfg["tcp_senders"]))]


def correr_repeticion(topo, esc, escenario_cfg, puertos, rep, dry=False):
    """Lanza TODOS los flujos (TCP stateful + UDP stateless) en paralelo."""
    rx_ip = CONFIG["ips"][CONFIG["hosts"][CONFIG["receptor"]]]
    dur = CONFIG["duracion_s"]
    outdir = CONFIG["dir_resultados"][topo]
    
    resultados = {}
    hilos = []
    lock = threading.Lock()
    
    def cliente_tcp(sender, puerto):
        host = get_host(sender)
        cmd = f"iperf3 -c {rx_ip} -p {puerto} -t {dur} -Z -J 2>&1"
        log(f"    TCP {sender} → {rx_ip}:{puerto}")
        out = host.cmd(cmd)
        nombre = f"{topo}_f5{esc}_tcp_{sender}{CONFIG['receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("tcp", sender)] = (out, nombre)
    
    def cliente_udp(sender):
        host = get_host(sender)
        payload = 1400
        cmd = (f"iperf3 -c {rx_ip} -p {CONFIG['udp_puerto']} -u "
           f"-b {CONFIG['udp_tasa']} -l {payload} -t {dur} -J 2>/dev/null")
        out = host.cmd(cmd)
        nombre = f"{topo}_f5{esc}_udp_{sender}{CONFIG['receptor']}_rep{rep:02d}.json"
        with lock:
            resultados[("udp", sender)] = (out, nombre)
    
    # Lanzar todos los clientes
    for i, s in enumerate(escenario_cfg["tcp_senders"]):
        t = threading.Thread(target=cliente_tcp, args=(s, puertos[i]))
        hilos.append(t)
        t.start()
    
    t_udp = threading.Thread(target=cliente_udp, args=(escenario_cfg["udp_sender"],))
    hilos.append(t_udp)
    t_udp.start()
    
    # Esperar a que terminen
    for h in hilos:
        h.join()
    
    if dry:
        return {"goodput_por_emisor": {}, "goodput_agregado_mbps": 0.0,
                "udp_jitter_ms": 0.0, "udp_perdida_pct": 0.0, "ok": True}
    
    # Parseo y guardado de JSONs
    medicion = {"goodput_por_emisor": {}, "ok": True}
    
    for (proto, sender), (out, nombre) in resultados.items():
        ruta = os.path.join(outdir, nombre)
        
        # Guardar salida cruda
        try:
            os.makedirs(outdir, exist_ok=True)
            with open(ruta, "w") as f:
                f.write(out)
        except OSError as e:
            log(f"  ✗ no se pudo guardar {nombre}: {e}")
        
        # Intentar parsear JSON
        if not out or not out.strip():
            log(f"  ✗ {proto.upper()} {sender}: Salida vacía")
            log(f"    Comando ejecutado en {sender}")
            medicion["ok"] = False
            continue
        
        # Verificar si la salida comienza con JSON (debe ser '{' o '[')
        out_stripped = out.strip()
        if not (out_stripped.startswith('{') or out_stripped.startswith('[')):
            log(f"  ✗ {proto.upper()} {sender}: Salida no es JSON")
            log(f"    Primeros 200 caracteres: {out_stripped[:200]}")
            medicion["ok"] = False
            continue
        
        try:
            data = json.loads(out_stripped)
            
            # Verificar si hay error en iperf3
            if "error" in data:
                log(f"  ✗ {proto.upper()} {sender}: iperf3 error: {data['error']}")
                medicion["ok"] = False
                continue
            
            if proto == "tcp":
                if "end" in data and "sum_received" in data["end"]:
                    bps = data["end"]["sum_received"]["bits_per_second"]
                    medicion["goodput_por_emisor"][sender] = bps / 1e6
                else:
                    log(f"  ✗ {proto.upper()} {sender}: Estructura JSON inesperada")
                    medicion["ok"] = False
            else:  # UDP
                if "end" in data and "sum" in data["end"]:
                    fin = data["end"]["sum"]
                    medicion["udp_jitter_ms"] = fin.get("jitter_ms", 0.0)
                    medicion["udp_perdida_pct"] = fin.get("lost_percent", 0.0)
                    log(f"    UDP {sender}: jitter={medicion['udp_jitter_ms']:.3f}ms, "
                        f"pérdida={medicion['udp_perdida_pct']:.2f}%")
                else:
                    log(f"  ✗ {proto.upper()} {sender}: Estructura JSON inesperada para UDP")
                    medicion["ok"] = False
                    
        except json.JSONDecodeError as e:
            log(f"  ✗ {proto.upper()} {sender}: JSON inválido - {e}")
            log(f"    Salida completa:\n{out_stripped[:500]}")
            medicion["ok"] = False
    
    if medicion["ok"]:
        medicion["goodput_agregado_mbps"] = sum(medicion["goodput_por_emisor"].values())
        medicion.setdefault("udp_jitter_ms", 0.0)
        medicion.setdefault("udp_perdida_pct", 0.0)
    
    return medicion


def correr_escenario(topo, esc, dry=False):
    cfg = CONFIG["escenarios"][esc]
    outdir = CONFIG["dir_resultados"][topo]
    os.makedirs(outdir, exist_ok=True)
    
    log(f"== F5 {esc.upper()} — Incast {len(cfg['tcp_senders'])}:1 → {CONFIG['receptor']} "
        f"(topología {topo}) ==")
    log(f"  TCP stateful: {', '.join(cfg['tcp_senders'])} | UDP stateless: {cfg['udp_sender']} "
        f"@ {CONFIG['udp_tasa']} / {CONFIG['udp_payload_bytes']}B")
    
    puertos = lanzar_servidores(cfg, dry=dry)
    
    goodputs, jitters = [], []
    reps_hechas = 0
    t0 = time.time()
    
    for rep in range(1, CONFIG["reps_max"] + 1):
        if not dry and rep > 1:
            log(f"  Enfriamiento de {CONFIG['enfriamiento_s']}s...")
            time.sleep(CONFIG["enfriamiento_s"])
        
        log(f"  Repetición {rep:02d}/{CONFIG['reps_max']}")
        m = correr_repeticion(topo, esc, cfg, puertos, rep, dry=dry)
        reps_hechas = rep
        
        if dry:
            break
        
        if not m["ok"]:
            log(f"  rep{rep:02d}: medición inválida — se repetirá el conteo sin esta rep")
            continue
        
        goodputs.append(m["goodput_agregado_mbps"])
        jitters.append(m["udp_jitter_ms"])
        log(f"  rep{rep:02d}: ✓ goodput = {m['goodput_agregado_mbps']:.1f} Mbps "
            f"({m['goodput_agregado_mbps']/CONFIG['line_rate_mbps']*100:.1f}% LR) | "
            f"jitter = {m['udp_jitter_ms']:.3f} ms | pérdida = {m['udp_perdida_pct']:.2f}%")
        
        if rep >= CONFIG["reps_min"]:
            r_g = rsd_pct(goodputs) if len(goodputs) > 1 else 100.0
            r_j = rsd_pct(jitters) if len(jitters) > 1 else 100.0
            log(f"  → RSD goodput = {r_g:.2f}% | RSD jitter = {r_j:.2f}% "
                f"(objetivo < {CONFIG['rsd_objetivo_pct']}%)")
            if r_g < CONFIG["rsd_objetivo_pct"] and r_j < CONFIG["rsd_objetivo_pct"]:
                log(f"  ✓ Convergencia alcanzada en {rep} repeticiones")
                break
    
    if not dry:
        rx = get_host(CONFIG["receptor"])
        rx.cmd("pkill -9 iperf3 2>/dev/null")
    
    if dry or not goodputs:
        return
    
    resumen = {
        "fase": "F5_incast", "escenario": esc, "topologia": topo,
        "timestamp": datetime.now().isoformat(),
        "receptor": CONFIG["receptor"],
        "tcp_senders": cfg["tcp_senders"], "udp_sender": cfg["udp_sender"],
        "duracion_s": CONFIG["duracion_s"], "repeticiones_validas": len(goodputs),
        "repeticiones_lanzadas": reps_hechas,
        "goodput_agregado_mbps": {
            "media": round(statistics.mean(goodputs), 2),
            "rsd_pct": round(rsd_pct(goodputs), 2),
            "pct_line_rate": round(statistics.mean(goodputs) / CONFIG["line_rate_mbps"] * 100, 2),
        },
        "udp_jitter_ms": {
            "media": round(statistics.mean(jitters), 4),
            "rsd_pct": round(rsd_pct(jitters), 2),
        },
        "duracion_total_min": round((time.time() - t0) / 60, 1),
        "odl": "OpenDaylight Vanadium 0.21.3 — proactivo, 1 camino activo",
    }
    
    ruta_resumen = os.path.join(outdir, f"{topo}_f5{esc}_resumen.json")
    with open(ruta_resumen, "w") as f:
        json.dump(resumen, f, indent=2)
    log(f"  Resumen guardado: {ruta_resumen}")


def iniciar_mininet(topo_type="sl", controller_ip="172.17.0.2"):
    """Inicializa la red Mininet con la topología especificada."""
    global net
    
    if topo_type == "sl":
        topo = SpineLeaf()
    elif topo_type == 'ft':
        topo = FatTree()
    else:
        raise NotImplementedError(f"Topología '{topo_type}' no esta implementada")
    
    controller = RemoteController('odl', ip=controller_ip, port=6653)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    net.start()

    # for host_name in CONFIG["hosts"]:
    #     host = net.get(host_name)
    #     CONFIG["ips"][host_name] = host.IP()
    #     print(f"Host {host_name} IP {host.IP()}")
    
    # Esperar a que los switches se conecten al controlador
    log("Esperando a que los switches se conecten al controlador...")
    time.sleep(5)
    
    # Configurar interfaces de loopback en todos los hosts
    for host_name, host_id in CONFIG["hosts"].items():
        host = net.get(host_id)
        host.cmd("ip link set lo up")
        host.cmd("ip link set lo up")  # Por si acaso
    
    # Verificar conectividad básica
    log("Verificando conectividad entre h1 y h7...")
    h1 = net.get("h1")
    h7 = net.get("h7")
    result = h1.cmd(f"ping -c 3 {CONFIG['ips']['h7']} -W 2")
    log(f"  Ping result: {result[:100]}")
    
    if "3 received" not in result:
        log("⚠ ADVERTENCIA: Ping entre h1 y h7 falló!")
        log("   Verifica que OpenDaylight esté corriendo y los flujos instalados")
    
    return net


# ============================================================
# Main
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Fase 5 — Incast Testing (RFC 8239 §6) - Mininet")
    ap.add_argument("--topology", choices=["sl", "j3c", "ft"], required=True,
                    help="sl = Spine-Leaf | j3c = Jerárquica 3 Capas")
    ap.add_argument("--escenario", choices=["s1", "s2"], default=None,
                    help="Correr solo un escenario (default: ambos)")
    ap.add_argument("--dry-run", action="store_true", help="Mostrar plan sin ejecutar")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--controller-ip", default="172.17.0.2", 
                    help="IP del controlador OpenDaylight")
    ap.add_argument("--debug", action="store_true", help="Habilitar modo debug")
    args = ap.parse_args()
    
    if args.topology == "j3c":
        print("Error: Topología jerárquica de 3 capas no implementada aún")
        sys.exit(1)
    
    escenarios = [args.escenario] if args.escenario else ["s1", "s2"]
    
    log(f"===== FASE 5 — INCAST (RFC 8239 §6) | topología={args.topology} "
        f"| escenarios={escenarios} | dry_run={args.dry_run} =====")
    
    # Iniciar Mininet si no es dry-run
    global net
    if not args.dry_run:
        log(f"Iniciando Mininet con topología {args.topology}...")
        try:
            net = iniciar_mininet(args.topology, args.controller_ip)
        except Exception as e:
            log(f"Error al iniciar Mininet: {e}")
            sys.exit(1)
    
    if not args.skip_preflight and not args.dry_run:
        cfg_total = {
            "tcp_senders": sorted({h for e in escenarios
                                   for h in CONFIG["escenarios"][e]["tcp_senders"]}),
            "udp_sender": CONFIG["escenarios"][escenarios[0]]["udp_sender"],
        }
        if not preflight(cfg_total, dry=args.dry_run):
            log("✗ Preflight falló. Corrige los problemas o usa --skip-preflight")
            if net:
                net.stop()
            sys.exit(1)
    
    for esc in escenarios:
        try:
            correr_escenario(args.topology, esc, dry=args.dry_run)
        except Exception as e:
            log(f"Error en escenario {esc}: {e}")
            if args.debug:
                import traceback
                traceback.print_exc()
    
    log("===== FASE 5 TERMINADA =====")
    
    if not args.dry_run and net:
        respuesta = input("\n¿Iniciar CLI de Mininet para inspección? (s/N): ")
        if respuesta.lower() == 's':
            CLI(net)
        net.stop()


if __name__ == "__main__":
    main()
