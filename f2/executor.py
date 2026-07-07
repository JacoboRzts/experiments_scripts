#!/usr/bin/env python3
"""
mininet_executor.py - Wrapper for Mininet host commands with serialization.
Avoids concurrent host.cmd() calls on the same host.
"""

import threading
import subprocess
import os
from types import SimpleNamespace

class MininetHostExecutor:
    """
    Encapsulates a Mininet Host object.
    Provides run() for foreground commands (serialized) and popen() for background.
    """
    def __init__(self, host):
        self.host = host
        self._lock = threading.Lock()

    def run(self, cmd, timeout=60):
        """
        Execute a command in foreground (blocking) using host.cmd().
        Returns an object similar to subprocess.CompletedProcess.
        """
        try:
            with self._lock:
                out = self.host.cmd(cmd, timeout=timeout)
            return SimpleNamespace(returncode=0, stdout=out, stderr="")
        except Exception as e:
            return SimpleNamespace(returncode=1, stdout="", stderr=str(e))

    def popen(self, cmd):
        """
        Launch a command in background using host.popen().
        Returns the Popen object (caller must wait/poll/kill).
        """
        with self._lock:
            # Set SHELL in the current process environment if not present
            # This is needed for Mininet's popen to work correctly
            old_shell = os.environ.get('SHELL')
            if old_shell is None:
                os.environ['SHELL'] = '/bin/sh'
            try:
                proc = self.host.popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            finally:
                # Restore original environment if we changed it
                if old_shell is None and 'SHELL' in os.environ:
                    del os.environ['SHELL']
                elif old_shell is not None:
                    os.environ['SHELL'] = old_shell
        return proc

    def kill_process(self, proc):
        """Terminate a previously launched background process."""
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()

    def kill_iperf(self):
        """Kill all iperf3 processes on this host."""
        self.run("pkill -9 iperf3 2>/dev/null; true", timeout=8)

    def IP(self):
        """Return the IP address of the host."""
        return self.host.IP()


def make_executors(net, mapping):
    """
    Create executors from Mininet net and a dict {name: host_object}.
    mapping example: {"H1": net.get('h1'), "H2": net.get('h2'), ...}
    Returns dict {name: MininetHostExecutor}.
    """
    return {name: MininetHostExecutor(host) for name, host in mapping.items()}
