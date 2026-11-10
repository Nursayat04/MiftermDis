"""PACELC benchmark: Linearizable (via Leaseholder) vs Follower Stale reads.

Starts a 3-replica cluster on localhost (three server.py processes). The client
always talks to Replica C, a follower:
  * LINEARIZABLE    -> C proxies the read to the Leaseholder (remote hop)
  * FOLLOWER_STALE  -> C answers from its local state

Second part: C is partitioned away from A and B and the same reads are repeated
to measure read availability during a partition.
"""
import math
import os
import socket
import statistics
import subprocess
import sys
import time

import grpc

import client
import kvstore_pb2 as pb

ROOT = os.path.dirname(os.path.abspath(__file__))
ITERATIONS = 1000
PARTITION_ITERATIONS = 200
MAX_STALENESS_MS = 5000
KEY = "bench-key"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_cluster():
    addrs = [f"127.0.0.1:{free_port()}" for _ in range(3)]
    procs = []
    for addr in addrs:
        procs.append(subprocess.Popen(
            [sys.executable, "server.py", "--port", addr.split(":")[1],
             "--cluster", ",".join(addrs), "--quiet"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    channels, stubs = [], []
    for addr in addrs:
        channel, stub = client.kv_connect(addr)
        grpc.channel_ready_future(channel).result(timeout=10)
        channels.append(channel)
        stubs.append(stub)
    return addrs, procs, channels, stubs


def partition_c(addrs, stubs, isolated):
    """Isolate C (index 2) from A and B, or heal the cluster."""
    for index, stub in enumerate(stubs):
        if not isolated:
            blocked = []
        elif index == 2:
            blocked = addrs[:2]
        else:
            blocked = [addrs[2]]
        client.kv_set_partition(stub, blocked)


def measure(stub, consistency, iterations):
    latencies, ok = [], 0
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            client.kv_get(stub, KEY, consistency, MAX_STALENESS_MS)
            latencies.append((time.perf_counter() - start) * 1000)
            ok += 1
        except grpc.RpcError:
            pass
    availability = 100.0 * ok / iterations
    if latencies:
        ordered = sorted(latencies)
        median = statistics.median(ordered)
        p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    else:
        median = p95 = float("nan")
    return median, p95, availability


def main():
    addrs, procs, channels, stubs = start_cluster()
    try:
        # seed one key through the leaseholder and wait until the cluster is warm
        deadline = time.time() + 10
        while time.time() < deadline:
            reply = client.kv_put(stubs[0], KEY, "value", pb.QUORUM_CP)
            if reply.success:
                break
            time.sleep(0.2)
        time.sleep(0.5)
        follower = stubs[2]

        for consistency in (pb.LINEARIZABLE, pb.FOLLOWER_STALE):  # warm-up
            measure(follower, consistency, 50)

        lin = measure(follower, pb.LINEARIZABLE, ITERATIONS)
        stale = measure(follower, pb.FOLLOWER_STALE, ITERATIONS)

        partition_c(addrs, stubs, isolated=True)
        time.sleep(0.5)
        lin_part = measure(follower, pb.LINEARIZABLE, PARTITION_ITERATIONS)
        stale_part = measure(follower, pb.FOLLOWER_STALE, PARTITION_ITERATIONS)
        partition_c(addrs, stubs, isolated=False)

        print(f"Cluster: 3 replicas on localhost, client targets Replica C (follower), "
              f"{ITERATIONS} reads per scenario, max_staleness_ms = {MAX_STALENESS_MS}\n")
        print("| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |")
        print("|---|---|---|---|---|")
        print(f"| Linearizable | Leaseholder (Remote) | {lin[0]:.2f} | {lin[1]:.2f} | {lin[2]:.1f} |")
        print(f"| Follower Stale | Local Follower | {stale[0]:.2f} | {stale[1]:.2f} | {stale[2]:.1f} |")
        print(f"\nDuring a partition (C isolated from A and B), {PARTITION_ITERATIONS} reads per scenario:\n")
        print("| Read Mode | Target Node | Read Availability (%) |")
        print("|---|---|---|")
        print(f"| Linearizable | Leaseholder (unreachable) | {lin_part[2]:.1f} |")
        print(f"| Follower Stale | Local Follower | {stale_part[2]:.1f} |")
    finally:
        for channel in channels:
            channel.close()
        for proc in procs:
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    main()
