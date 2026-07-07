#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.topo import Topo

NODES = [
    "2977893393545632",
    "2977893393545536",
    "2977893393522176",
    "2977893393526016",
    "2977893393165440",
]

class SpineLeaf(Topo):
    def __init__(self, n_spine=2, n_leaf=3, n_host=3, **opts):
        super(SpineLeaf, self).__init__(**opts)
        spines = []
        leafs = []
        for i in range(1, n_spine + 1):
            spine = self.addSwitch(f's{i}', protocols='OpenFlow13', dpid=NODES[i-1])
            print(f"Switch s{i} added with dpid {NODES[i-1]}")
            spines.append(spine)
        for j in range(1, n_leaf + 1):
            leaf = self.addSwitch(f's{j+n_spine}', protocols='OpenFlow13', dpid=NODES[j+1])
            print(f"Switch s{j} added with dpid {NODES[j+2]}")
            leafs.append(leaf)
            for spine_idx, spine in enumerate(spines, start=2):
                self.addLink(leaf, spine,
                            port1=spine_idx,
                            port2=j + 1, bw=1000, delay='1ms') 
            for k in range(1, n_host + 1):
                host_id = (j-1) * n_host + k
                ip = f"10.0.{j}.{k}/16"
                host = self.addHost(f'h{host_id}', ip=ip)
                self.addLink(host, leaf, port2=k+12, bw=1000, delay='1ms')

class FatTree(Topo):
    def __init__(self, n_core: int = 1, n_aggr: int = 2, n_edge: int = 2, n_host: int = 4, **opts):
        super(FatTree, self).__init__(**opts)
        core_list = []
        aggr_list = []
        edge_list = []
        for i in range(1, n_core + 1):
            core_list.append(self.addSwitch(f"s{i}", protocol='OpenFlow13'))
        for j in range(n_aggr):
            aggr = self.addSwitch(f"s{n_core + j + 1}", protocol='OpenFlow13')
            aggr_list.append(aggr)
            for idx, core in enumerate(core_list):
                self.addLink(
                    aggr, core,
                    port1=2,
                    port2=j + 2,
                    bw=1000, delay='1ms'
                )
        for k in range(n_edge):
            edge = self.addSwitch(f"s{n_core + n_aggr + k + 1}", protocol='OpenFlow13')
            edge_list.append(edge)
            idx = k % len(aggr_list)
            aggr = aggr_list[idx]
            self.addLink(
                edge, aggr,
                port1=2,
                port2=3,
                bw=1000, delay='1ms'
            )
            for i in range(1, n_host + 1):
                ip = f'10.0.{k + 1}.{i}/16'
                host_id = k * n_host + i
                host = self.addHost(f"h{host_id}", ip=ip)
                self.addLink(
                    host, edge,
                    port2=12 + i,
                    bw=1000, delay='1ms'
                )
        
        edge = edge_list[-1]
        for i in range(1, 3):
            host = self.addHost(f"h{i+8}", ip=f"10.0.2.{i+4}/16")
            self.addLink(host, edge, port2=16+i, bw=1000, delay='1ms')

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Define a topology in mininet.")
    parser.add_argument("-t", "--topology", type=str, default='sl', help="Topology to test")
    args = parser.parse_args()

    match args.topology:
        case "sl":
            topo = SpineLeaf()
        case "ft":
            topo = FatTree()
        case _:
            print(f"Topology {args.topology} don't exists.")
            return 0
    controller = RemoteController('odl', ip="172.17.0.2", port=6653)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    net.start()
    CLI(net)

if __name__ == "__main__":
    main()
