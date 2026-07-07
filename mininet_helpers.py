#!/usr/bin/env python3
"""
mininet_helpers.py - Mininet helper functions and executor class
"""

import os
import time
from pathlib import Path
from typing import List, Optional

from mininet.net import Mininet
from mininet.cli import CLI
from mininet.log import info, error, warn, setLogLevel
from mininet.node import OVSSwitch, RemoteController
from mininet.link import TCLink

from topology import SpineLeaf, FatTree

# Global verbose flag
VERBOSE = False

def set_verbose(verbose: bool):
    """Set verbose mode for Mininet helpers"""
    global VERBOSE
    VERBOSE = verbose
    if verbose:
        setLogLevel('info')
    else:
        setLogLevel('warning')
        # Suppress HTB warnings
        import warnings
        warnings.filterwarnings("ignore", category=UserWarning)


class MininetExecutor:
    """Manages command execution on Mininet hosts"""

    def __init__(self, net: Mininet):
        self.net = net
        self.processes = []
        self.host_cache = {}

        # Create host cache for fast access
        for host in net.hosts:
            self.host_cache[host.name] = host

    def get_host(self, host_key: str):
        """Get Mininet host object"""
        mininet_name = host_key.lower()
        if mininet_name not in self.host_cache:
            try:
                host = self.net.get(mininet_name)
                self.host_cache[mininet_name] = host
                return host
            except:
                raise ValueError(f"Host {host_key} not found in network")
        return self.host_cache[mininet_name]

    def run_cmd(self, host_key: str, cmd: str, timeout: int = 90):
        """Execute a command on a host and return result"""
        host = self.get_host(host_key)

        try:
            if 'iperf3' in cmd and '-t' in cmd:
                # iperf3 with timeout - run and wait
                proc = host.popen(cmd, shell=True)
                try:
                    stdout, stderr = proc.communicate(timeout=timeout)
                    returncode = proc.returncode
                except Exception as e:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    returncode = -1
                    stderr = str(e) if not stderr else stderr
            else:
                result = host.cmd(cmd)
                stdout = result
                stderr = ""
                returncode = 0

            class ProcessResult:
                def __init__(self, stdout, stderr, returncode):
                    self.stdout = stdout
                    self.stderr = stderr
                    self.returncode = returncode

            return ProcessResult(stdout, stderr, returncode)

        except Exception as e:
            if VERBOSE:
                error(f"Error executing command on {host_key}: {e}\n")
            class ProcessResult:
                def __init__(self):
                    self.stdout = ""
                    self.stderr = str(e)
                    self.returncode = -1
            return ProcessResult()

    def run_bg(self, host_key: str, cmd: str):
        """Execute a command in background"""
        host = self.get_host(host_key)

        if 'SHELL' not in os.environ:
            os.environ['SHELL'] = '/bin/bash'

        try:
            proc = host.popen(cmd, shell=True)
            self.processes.append(proc)
            return proc
        except KeyError as e:
            if VERBOSE:
                warn(f"Error in popen: {e}. Trying alternative method...\n")
            proc = host.popen(['/bin/bash', '-c', cmd])
            self.processes.append(proc)
            return proc

    def kill_iperf(self, host_key: str):
        """Kill iperf3 processes on a host"""
        self.run_cmd(host_key, "pkill -9 iperf3 2>/dev/null; true", timeout=5)

    def kill_iperf_all(self, hosts: List[str]):
        """Kill iperf3 on both clients and servers"""
        for h in hosts:
            self.kill_iperf(h)

    def cleanup(self):
        """Clean up background processes"""
        for proc in self.processes:
            try:
                if proc.poll() is None:
                    proc.kill()
            except:
                pass
        self.processes = []


def configure_tcp(executor: MininetExecutor, congestion: str = "cubic"):
    """Configure TCP on all hosts"""
    for host_key in executor.host_cache.keys():
        # Convert mininet name back to host key
        host_key_upper = host_key.upper()
        executor.run_cmd(host_key_upper, 
                        f"sysctl -w net.ipv4.tcp_congestion_control={congestion}")
        executor.run_cmd(host_key_upper, 
                        "sysctl -w net.ipv4.tcp_no_metrics_save=1")
        executor.run_cmd(host_key_upper, 
                        "sysctl -w net.ipv4.tcp_slow_start_after_idle=0")


def create_network(topology_type: str, controller_ip: str = None):
    """
    Create and return a Mininet network with specified topology
    """
    if topology_type.lower() == 'sl':
        topo = SpineLeaf()
    elif topology_type.lower() == 'ft':
        topo = FatTree()
    else:
        raise ValueError(f"Unsupported topology: {topology_type}")

    # Suppress HTB warnings if not verbose
    if not VERBOSE:
        import warnings
        warnings.filterwarnings("ignore", message=".*sch_htb.*")
        warnings.filterwarnings("ignore", message=".*quantum.*")

    if controller_ip:
        controller = RemoteController('odl', ip=controller_ip, port=6653)
        net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, 
                     controller=controller)
    else:
        net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch)

    return net


def preflight(executor: MininetExecutor, hosts: List[str]) -> bool:
    """Check if iperf3 is available on all hosts"""
    if VERBOSE:
        info("-- Preflight check " + "-"*52 + "\n")
    
    all_ok = True
    for name in hosts:
        res = executor.run_cmd(name, "iperf3 --version 2>&1 | head -1", timeout=10)
        ok = res.returncode == 0
        ver = res.stdout.strip()[:55] if ok else res.stderr.strip()[:55]
        if VERBOSE:
            info(f"  {'OK' if ok else 'FAIL'} {name:4s}  {ver}\n")
        if not ok:
            all_ok = False
    
    if VERBOSE:
        info("\n")
    return all_ok
