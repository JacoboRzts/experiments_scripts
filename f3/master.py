#!/usr/bin/env python3
"""
F3 Mininet
"""

import argparse
import json
import statistics
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from topology import SpineLeaf, FatTree

EXPERIMENT      = "f3"
LINE_RATE_MBPS  = 1000

DURATION        = 30
COOLDOWN        = 5
RATE_PAUSE      = 10
RSD_TARGET      = 10.0
REPS_MIN        = 15
REPS_MAX        = 30

UDP_RATES       = [10, 50, 100, 500, 900]

OUTPUT_BASE     = Path.home() / "experimentos"

CONTROLLER_IP   = "172.17.0.2"
CONTROLLER_PORT = 6653

N_SPINE = 2
N_LEAF  = 3
N_HOST  = 3

SCENARIO = {
    "emisores":  ["H1", "H4", "H7"],
    "destino":   "H8",
    "puertos":   {"H1": 5301, "H4": 5302, "H7": 5303},
}


# ============================================================================
#  TERMINAL OUTPUT
# ============================================================================

class C:
    OK   = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    END  = "\033[0m"


def ok(msg):   print(f"  {C.OK}[OK]{C.END}  {msg}")
def warn(msg): print(f"  {C.WARN}[WARN]{C.END}  {msg}")
def fail(msg): print(f"  {C.FAIL}[FAIL]{C.END}  {msg}")
def info(msg): print(f"  ->  {msg}")


# ============================================================================
#  MININET HOST HELPERS
# ============================================================================

def get_host_by_key(net, host_key):
    mapping = {
        "H1": net.get('h1'),
        "H4": net.get('h4'),
        "H7": net.get('h7'),
        "H8": net.get('h8'),
    }
    return mapping.get(host_key)


def kill_iperf_on_host(host):
    """Aggressively kill all iperf3 processes on a host."""
    host.cmd("killall -9 iperf3 2>/dev/null; pkill -9 iperf3 2>/dev/null; true")


def start_servers(net, dry_run):
    """Start iperf3 UDP servers on H8 with isolated log files."""
    if dry_run:
        return

    h8 = get_host_by_key(net, "H8")
    
    # Clean up any existing servers
    kill_iperf_on_host(h8)
    time.sleep(1)
    
    # Start one server per port with separate log files to avoid output mixing
    for emisor, puerto in SCENARIO["puertos"].items():
        log_file = f"/tmp/iperf3_server_{puerto}.log"
        h8.cmd(f"iperf3 -s -p {puerto} > {log_file} 2>&1 &")
    
    time.sleep(2)
    
    # Verify servers are running
    result = h8.cmd("ps aux | grep iperf3 | grep -v grep | wc -l")
    server_count = int(result.strip())
    if server_count == len(SCENARIO["puertos"]):
        ok(f"UDP servers active on H8 ({server_count} servers)")
    else:
        warn(f"Only {server_count}/{len(SCENARIO['puertos'])} servers running")


def stop_servers(net, dry_run):
    """Stop all iperf3 servers on H8."""
    if not dry_run:
        h8 = get_host_by_key(net, "H8")
        kill_iperf_on_host(h8)


# ============================================================================
#  UDP FLOW EXECUTION
# ============================================================================

def run_udp_flow(host, dest_host, rate_mbps, pkt_size, port, results, errors, dry_run):
    """
    Execute iperf3 UDP flow from host to dest_host.
    Extract jitter_ms (RFC 1889) from JSON output.
    Uses timeout wrapper to prevent hanging processes.
    """
    if dry_run:
        results[host.name] = {
            "jitter_ms": round(0.1 + rate_mbps * 0.0001, 4),
            "lost_packets": 0,
            "throughput_mbps": rate_mbps * 0.98,
        }
        return

    ip_destino = dest_host.IP()
    
    # Add timeout wrapper to prevent hanging (duration + 5 seconds grace)
    cmd = (f"timeout {DURATION + 5} iperf3 -c {ip_destino} -p {port} "
           f"-u -b {rate_mbps}M -l {pkt_size} -t {DURATION} -J 2>/dev/null")

    try:
        output = host.cmd(cmd)

        if not output or len(output.strip()) == 0:
            errors.append(f"{host.name}: iperf3 returned empty output")
            results[host.name] = None
            return

        # Clean output - sometimes iperf3 prints extra lines before JSON
        # Find the first '{' character to start JSON parsing
        json_start = output.find('{')
        if json_start == -1:
            errors.append(f"{host.name}: no JSON found in output")
            results[host.name] = None
            return
        
        clean_output = output[json_start:]
        data = json.loads(clean_output)
        
        end = data.get("end", {})
        s = end.get("sum", {})

        jitter_ms = s.get("jitter_ms")
        lost_packets = s.get("lost_packets", 0)
        throughput_bps = s.get("bits_per_second", 0)

        results[host.name] = {
            "jitter_ms": round(jitter_ms, 4) if jitter_ms is not None else None,
            "lost_packets": lost_packets,
            "throughput_mbps": round(throughput_bps / 1e6, 2),
        }

    except json.JSONDecodeError as e:
        errors.append(f"{host.name}: invalid JSON - {e}")
        results[host.name] = None
    except Exception as e:
        errors.append(f"{host.name}: error - {str(e)[:50]}")
        results[host.name] = None


# ============================================================================
#  STATISTICS FUNCTIONS
# ============================================================================

def compute_rsd(values):
    """Calculate Relative Standard Deviation as percentage."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100


def nombre_archivo(topology, rate_mbps, rep):
    """Generate filename for individual run JSON."""
    return f"{topology}_{EXPERIMENT}_udp_rate{rate_mbps:04d}_rep{rep:02d}.json"


# ============================================================================
#  RATE-SPECIFIC TESTING LOOP
# ============================================================================

def run_rate(net, topology, rate_mbps, out_dir, dry_run):
    """
    Run 15-30 repetitions of 3 simultaneous UDP flows at a given rate.
    Returns jitter statistics.
    """
    PKT_SIZE = 1024

    jitters_h1 = []
    jitters_h4 = []
    jitters_h7 = []
    jitters_avg = []

    rep = 0
    converged = False

    if not dry_run:
        dest_host = get_host_by_key(net, "H8")
        emisores_map = {
            "H1": get_host_by_key(net, "H1"),
            "H4": get_host_by_key(net, "H4"),
            "H7": get_host_by_key(net, "H7"),
        }

    print(f"\n  {C.BOLD}-- Rate: {rate_mbps} Mbps "
          f"({(rate_mbps/LINE_RATE_MBPS)*100:.0f}% LR) --{C.END}")

    while rep < REPS_MAX:
        rep += 1

        # Cooldown period between repetitions
        if rep > 1:
            for i in range(COOLDOWN, 0, -1):
                print(f"\r  Rep {rep:02d}/{REPS_MAX} cooling {i}s...   ",
                      end="", flush=True)
                time.sleep(1)

        print(f"\r  Rep {rep:02d}/{REPS_MAX} measuring {rate_mbps}Mbps...  ",
              end="", flush=True)

        if dry_run:
            # Simulated data for dry-run
            sim_jitter = 0.02 + (rate_mbps / 10000)
            results = {
                "h1": {"jitter_ms": sim_jitter},
                "h4": {"jitter_ms": sim_jitter + 0.005},
                "h7": {"jitter_ms": sim_jitter + 0.01},
            }
            errors = []
        else:
            results = {}
            errors = []
            threads = []
            
            # Launch 3 parallel UDP flows
            for host_key, host_obj in emisores_map.items():
                port = SCENARIO["puertos"][host_key]
                t = threading.Thread(
                    target=run_udp_flow,
                    args=(host_obj, dest_host, rate_mbps, PKT_SIZE,
                          port, results, errors, dry_run)
                )
                threads.append(t)
                t.start()

            # Wait for all flows to complete
            for t in threads:
                t.join(timeout=DURATION + 30)

        # Display any errors that occurred
        for e in errors:
            print(f"\n    {C.WARN}[WARN]{C.END} {e}", end="")

        # Extract jitter values from each emitter
        j_h1 = results.get("h1", {})
        j_h4 = results.get("h4", {})
        j_h7 = results.get("h7", {})

        jitter_vals = []
        
        if j_h1 and j_h1.get("jitter_ms") is not None:
            jitter_vals.append(j_h1["jitter_ms"])
            jitters_h1.append(j_h1["jitter_ms"])
            
        if j_h4 and j_h4.get("jitter_ms") is not None:
            jitter_vals.append(j_h4["jitter_ms"])
            jitters_h4.append(j_h4["jitter_ms"])
            
        if j_h7 and j_h7.get("jitter_ms") is not None:
            jitter_vals.append(j_h7["jitter_ms"])
            jitters_h7.append(j_h7["jitter_ms"])

        # Calculate average jitter for this repetition
        if jitter_vals:
            rep_jitter_avg = statistics.mean(jitter_vals)
            jitters_avg.append(rep_jitter_avg)
            j_str = f"{rep_jitter_avg:.3f}ms"
        else:
            rep_jitter_avg = None
            j_str = "N/A"

        # Calculate RSD for convergence check
        rsd = compute_rsd(jitters_avg)
        rsd_str = f"{rsd:.1f}%" if rsd is not None else "N/A"

        # Color coding based on convergence status
        if rsd is not None and rsd < RSD_TARGET:
            status_color = C.OK
        elif rsd is not None:
            status_color = C.WARN
        else:
            status_color = C.END

        print(f"\r  Rep {rep:02d}/{REPS_MAX} "
              f"jitter_avg={j_str} "
              f"RSD={status_color}{rsd_str}{C.END}   ")

        # Save individual run JSON
        if not dry_run:
            fname = nombre_archivo(topology, rate_mbps, rep)
            record = {
                "_meta": {
                    "experiment": EXPERIMENT,
                    "topology": topology,
                    "rate_mbps": rate_mbps,
                    "pkt_size_b": PKT_SIZE,
                    "rep": rep,
                    "duration_s": DURATION,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "rfc_reference": "RFC1889 jitter / RFC8239 section F3",
                    "emisores": SCENARIO["emisores"],
                    "destino": SCENARIO["destino"],
                },
                "H1": results.get("h1"),
                "H4": results.get("h4"),
                "H7": results.get("h7"),
            }
            (out_dir / fname).write_text(json.dumps(record, indent=2))

        # Check convergence criteria
        if rep >= REPS_MIN and rsd is not None and rsd < RSD_TARGET:
            converged = True
            break

    if not converged and not dry_run:
        warn(f"RSD did not converge in {REPS_MAX} reps for {rate_mbps} Mbps")

    def safe_stats(vals):
        clean = [v for v in vals if v is not None]
        if len(clean) < 2:
            return None, None, None, None
        return (round(min(clean), 4), round(statistics.mean(clean), 4),
                round(max(clean), 4), round(compute_rsd(clean) or 0, 2))

    j_min, j_avg, j_max, j_rsd = safe_stats(jitters_avg)

    if j_avg is not None:
        info(f"jitter min/avg/max: {j_min}/{j_avg}/{j_max} ms | RSD: {j_rsd}% | reps: {rep}")

    h1_avg = round(statistics.mean(jitters_h1), 4) if jitters_h1 else None
    h4_avg = round(statistics.mean(jitters_h4), 4) if jitters_h4 else None
    h7_avg = round(statistics.mean(jitters_h7), 4) if jitters_h7 else None

    return {
        "rate_mbps": rate_mbps,
        "lr_pct": round((rate_mbps / LINE_RATE_MBPS) * 100, 1),
        "reps": rep,
        "converged": converged,
        "jitter_min_ms": j_min,
        "jitter_avg_ms": j_avg,
        "jitter_max_ms": j_max,
        "jitter_rsd_pct": j_rsd,
        "jitter_h1_avg": h1_avg,
        "jitter_h4_avg": h4_avg,
        "jitter_h7_avg": h7_avg,
    }


# ============================================================================
#  COMPLETE EXPERIMENT
# ============================================================================

def run_experiment(topology, rates, dry_run):
    """Main experiment orchestration using Mininet topology."""
    out_dir = OUTPUT_BASE / topology / "fase3_jitter"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*65}")
    print(f"  F3 JITTER (MININET) | {topology.upper()}")
    print(f"  Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print( "  Emitters: H1 + H4 + H7  |  Destination: H8")
    print(f"  Rates: {rates} Mbps")
    print(f"  Duration: {DURATION}s per run")
    print(f"  Reps: {REPS_MIN}-{REPS_MAX} (RSD < {RSD_TARGET}%)")
    print(f"  Output: {out_dir}")
    if dry_run:
        print("  MODE: DRY-RUN")
    print(f"{'='*65}\n")

    net = None

    if not dry_run:
        print("-- Building topology " + "-"*50)
        controller = RemoteController('odl', ip=CONTROLLER_IP, port=CONTROLLER_PORT)

        if topology == "sl":
            topo = SpineLeaf()
        elif topology == "ft":
            topo = FatTree()
        else:
            print(f"  [FAIL] Topology '{topology}' not recognized")
            sys.exit(1)

        net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
        net.start()

        print("\n-- Connectivity test " + "-"*50)
        net.pingAll()

        h1 = net.get('h1')
        h4 = net.get('h4')
        h7 = net.get('h7')
        h8 = net.get('h8')

        print( "\n  Host assignments:")
        print(f"    H1 (emitter leaf1): {h1.IP()}")
        print(f"    H4 (emitter leaf2): {h4.IP()}")
        print(f"    H7 (emitter leaf3): {h7.IP()}")
        print(f"    H8 (destination):   {h8.IP()}")
        print()

        start_servers(net, dry_run)

    all_results = []

    for i, rate in enumerate(rates):
        if dry_run:
            result = run_rate(None, topology, rate, out_dir, dry_run)
        else:
            result = run_rate(net, topology, rate, out_dir, dry_run)
        all_results.append(result)

        # Pause between rate groups (except after last)
        if i < len(rates) - 1 and not dry_run:
            info(f"Pausing {RATE_PAUSE}s before next rate...")
            time.sleep(RATE_PAUSE)

    if not dry_run and net is not None:
        stop_servers(net, dry_run)
        net.stop()

    # Print summary table
    print(f"\n\n{'='*65}")
    print(f"  F3 JITTER SUMMARY - {topology.upper()}")
    print(f"  {'Rate':>6}  {'LR%':>6}  {'Jitter min':>12}  "
          f"{'Jitter avg':>12}  {'Jitter max':>12}  {'RSD%':>7}  {'Reps':>5}")
    print(f"  {'-'*6}  {'-'*6}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*7}  {'-'*5}")

    for r in all_results:
        rfc_ok = (r["jitter_rsd_pct"] or 99) < RSD_TARGET
        rsd_str = f"{r['jitter_rsd_pct']:.1f}%" if r["jitter_rsd_pct"] else "N/A"
        flag = f"{C.OK}[OK]{C.END}" if rfc_ok else f"{C.WARN}[WARN]{C.END}"

        min_str = f"{r['jitter_min_ms']}ms" if r['jitter_min_ms'] else "N/A"
        avg_str = f"{r['jitter_avg_ms']}ms" if r['jitter_avg_ms'] else "N/A"
        max_str = f"{r['jitter_max_ms']}ms" if r['jitter_max_ms'] else "N/A"

        print(f"  {r['rate_mbps']:>5}M  {r['lr_pct']:>5.0f}%"
              f"  {min_str:>12}"
              f"  {avg_str:>12}"
              f"  {max_str:>12}"
              f"  {rsd_str:>7} {flag}"
              f"  {r['reps']:>5}")
    print(f"{'='*65}\n")

    # Save summary JSON
    if not dry_run:
        summary_path = out_dir / f"{topology}_{EXPERIMENT}_summary.json"
        summary_path.write_text(json.dumps({
            "experiment": EXPERIMENT,
            "topology": topology,
            "rfc_reference": "RFC1889 jitter / RFC8239 section F3",
            "emisores": SCENARIO["emisores"],
            "destino": SCENARIO["destino"],
            "udp_rates_mbps": rates,
            "duration_s": DURATION,
            "rsd_target": RSD_TARGET,
            "reps_min": REPS_MIN,
            "reps_max": REPS_MAX,
            "line_rate_mbps": LINE_RATE_MBPS,
            "controller_ip": CONTROLLER_IP,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "results": all_results,
        }, indent=2))
        ok(f"Summary saved: {summary_path}")


# ============================================================================
#  ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="F3: Jitter Testing with Mininet - H1+H4+H7 -> H8 (UDP)"
    )
    parser.add_argument("--topology", choices=["sl", "ft"], default="sl",
                        help="Topology: sl (spine-leaf) or ft (fat-tree)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate execution without actual traffic")
    parser.add_argument("--rates", nargs="+", type=int, default=UDP_RATES,
                        help=f"UDP rates in Mbps (default: {UDP_RATES})")
    args = parser.parse_args()

    run_experiment(
        topology=args.topology,
        rates=args.rates,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
