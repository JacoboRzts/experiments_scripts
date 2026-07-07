#!/usr/bin/env python3
"""
f1_master_mininet.py — Orquestador F1 para Mininet
Ejecuta A1, A2, B1, B2 en secuencia.
Uso: sudo python3 f1_master_mininet.py [--topology ft] [--dry-run]
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime

SCRIPTS = {
    "a1": "f1/a1.py",
    "a2": "f1/a2.py",
    "b1": "f1/b1.py",
    "b2": "f1/b2.py",
}
ORDER = ["a1", "a2", "b1", "b2"]
PAUSE = 15

def run_script(script, topology, dry_run):
    cmd = ["python3", script, "--topology", topology]
    if dry_run:
        cmd.append("--dry-run")
    print(f"\n--- Ejecutando {script} ---")
    return subprocess.call(cmd) == 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", nargs="+", choices=ORDER)
    args = parser.parse_args()

    to_run = ORDER if not args.only else args.only

    print(f"\n{'='*60}")
    print(f"  F1 MASTER (Mininet) | {args.topology.upper()} | {datetime.now()}")
    print(f"  Secuencia: {' → '.join(to_run)}")
    if args.dry_run:
        print("  MODO DRY-RUN")
    print(f"{'='*60}")

    results = {}
    for exp in to_run:
        ok = run_script(SCRIPTS[exp], args.topology, args.dry_run)
        results[exp] = ok
        if ok and exp != to_run[-1] and not args.dry_run:
            print(f"\n  Pausa de {PAUSE}s...")
            time.sleep(PAUSE)

    print(f"\n{'='*60}")
    print("  RESUMEN")
    for exp in to_run:
        status = "✓" if results[exp] else "✗"
        print(f"  {status} {exp.upper()}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
