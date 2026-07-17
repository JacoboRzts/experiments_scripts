
import time
import statistics
import argparse

# Colors
class C:
    GREEN = "\033[32m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[0;34m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"

# Custom messages

def ok(msg: str, end: str = "\n"):
    print(f"{C.GREEN}  OK {msg}{C.END}", end=end)

def info(msg, end="\n", start=" "):
    print(f"{start} {C.BLUE}INFO {msg}{C.END}", end=end)

def warn(msg, end="\n", start=" "):
    print(f"{start} {C.YELLOW}WARNING{C.END} {msg}", end=end)

def error(msg, end="\n", start=" "):
    print(f"{start} {C.RED}ERROR{C.END} {msg}", end=end)

def title(title):
    print('\n'+'─'*64)
    print(f"  {title}")
    print('─'*64,'\n')

# Checkers

def check_connectivity(client, server_ip, port) -> bool:
    cmd = f"nc -zv {server_ip} {port} 2>&1"
    result = client.cmd(cmd)
    result = result.lower()
    return "succeeded" not in result and "connected" not in result

def check_iperf(host) -> bool:
    """"Check if the given host has running any iperf process."""
    result = host.cmd("pgrep -f iperf3 2>/dev/null")
    return bool(result.strip())

def preflight(hosts) -> bool:
    status = True
    tools = ["netcat", "iperf3"]
    info("Checking all tools are installed on each host.")
    for host in hosts:
        for tool in tools:
            output = host.cmd(f"{tool} --version 2>&1")
            status = bool(output.strip())
            if not status:
                warn(f"{tool} is not installed on host {host.name}")
    return status

# NetTools

def kill_iperf(host, timeout: int=1):
    host.cmd("pkill -9 iperf3")
    time.sleep(timeout)

def start_iperf_server(server, port, timeout=1):
    cmd = f"iperf3 -s -p {port} -D"
    server.cmd(cmd)
    time.sleep(timeout)

# Stats

def rsd(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

# Arugments

def getArguments(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-e", "--experiment", default="all", help="Experiments to execute.")
    parser.add_argument("-t", "--topology", choices=["sl", "j3c"], default="sl", help="Topology to use.")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-s", "--skip-preflight", action="store_true")
    parser.add_argument("-c", "--controller-ip", default="172.17.0.2", help="IP address of the controller.")
    args = parser.parse_args()
    return args

