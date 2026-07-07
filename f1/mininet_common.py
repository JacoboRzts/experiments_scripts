#!/usr/bin/env python3
"""
F1 Common Functions Mininet
"""

import sys
import os
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from mininet.net import Mininet
from mininet.link import TCLink
from mininet.log import info
from mininet.node import RemoteController, OVSSwitch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from topology import SpineLeaf, FatTree

# ------------------------------------------------------------
# Configuración
# ------------------------------------------------------------
DURATION = 30
PKT_SIZES = [64, 128, 256, 512, 1024, 1518]
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
COOLDOWN = 3
PKT_PAUSE = 10

ODL_IP = "172.17.0.2" 
ODL_PORT = 6653

OUTPUT_BASE = Path.home() / "experimentos"
EXPERIMENT = None

# ------------------------------------------------------------
# Creación de la red según topología elegida
# ------------------------------------------------------------
def create_network(topology='sl'):
    """
    Crea y arranca la red Mininet.
    """
    if topology == 'sl':
        topo = SpineLeaf()
    elif topology == 'ft':
        topo = FatTree()
    else:
        raise ValueError(f"Topología '{topology}' no soportada")

    controller = RemoteController('odl', ip=ODL_IP, port=ODL_PORT)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    net.start()
    return net

def iperf_server_start(host, port):
    """Inicia servidor iperf3 en background"""
    # Matar cualquier instancia previa en ese puerto
    host.cmd(f"pkill -9 -f 'iperf3.*-p {port}'")
    time.sleep(0.5)
    
    # Iniciar en modo daemon (-D) o con nohup
    cmd = f"iperf3 -s -p {port} -D"
    result = host.cmd(cmd)
    
    # Verificar que empezó
    time.sleep(1)
    check = host.cmd(f"ss -tlnp | grep {port}")
    if check:
        print(f"  Servidor iperf3 iniciado en {host.name}:{port}")
        return True
    else:
        print(f"  ERROR: No se pudo iniciar servidor en {host.name}:{port}")
        return False

def iperf_server_kill(host):
    host.cmd('pkill -9 iperf3')
    time.sleep(0.5)

def iperf_client_run(client, server_ip, port, pkt_size, duration):
    cmd = (f"iperf3 -c {server_ip} -p {port} -t {duration} "
           f"-Z -l {pkt_size} -J")
    result = client.cmd(cmd)
    try:
        data = json.loads(result)
        mbps = data['end']['sum_sent']['bits_per_second'] / 1e6
        return data, mbps
    except (json.JSONDecodeError, KeyError, TypeError):
        return None, None

def extract_mbps(data):
    try:
        return data['end']['sum_sent']['bits_per_second'] / 1e6
    except (KeyError, TypeError):
        return None

def inject_meta(data, pair_id, pkt_size, rep, topology, experiment):
    data['_meta'] = {
        'experiment':    experiment,
        'topology':      topology,
        'pair_id':       pair_id,
        'pkt_size_b':    pkt_size,
        'rep':           rep,
        'duration_s':    DURATION,
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'rfc_reference': 'RFC8239 §2 — Line-Rate Testing (Mininet + topología externa)',
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

def stop_network(net):
    net.stop()

def preflight():
    import shutil
    if not shutil.which('iperf3'):
        print("ERROR: iperf3 no está instalado. Ejecuta: sudo apt install iperf3")
        return False
    return True
