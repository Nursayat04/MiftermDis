import math
import os
import statistics
import sys
import threading
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client import incr, quorum_incr
from test_failures import start_replica

REQUESTS = 2000


def run(label, call, clients):
    per_client = REQUESTS // clients
    latencies = []
    lock = threading.Lock()

    def worker():
        mine = []
        for _ in range(per_client):
            key = str(uuid.uuid4())
            start = time.perf_counter()
            call(key)

            # TODO 1
            mine.append((time.perf_counter() - start) * 1000)

        with lock:
            latencies.extend(mine)

    threads = [threading.Thread(target=worker) for _ in range(clients)]

    # TODO 2
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    n = len(latencies)

    # TODO 3
    median = statistics.median(latencies)
    sorted_latencies = sorted(latencies)
    p95 = sorted_latencies[math.ceil(0.95 * n) - 1]

    print(f"| {label} | {median:.2f} | {p95:.2f} | {n} |")


def main():
    items = [start_replica(n) for n in ("replica-A", "replica-B", "replica-C")]
    stubs = [item[1] for item in items]

    try:
        print("| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
        print("|---|---|---|---|")

        one = lambda key: incr(stubs[0], "x", 1, key)
        three = lambda key: quorum_incr(stubs, "x", 1, key)

        run("Single replica, 1 client", one, 1)
        run("Single replica, 16 clients", one, 16)
        run("Quorum (3 replicas), 1 client", three, 1)
        run("Quorum (3 replicas), 16 clients", three, 16)

    finally:
        for proc, stub, channel in items:
            channel.close()
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    main()