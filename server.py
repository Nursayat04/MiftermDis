import argparse
import threading
from concurrent import futures
from clocks import LamportClock

import grpc
import counter_pb2
import counter_pb2_grpc
import kvstore_pb2_grpc
from kvstore_service import KVServicer, DEFAULT_CLUSTER
import time


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A", delay_ms=0):
        self.name = name
        self.clock = LamportClock(name)
        self.delay_ms = delay_ms
        self._delayed = False
        self._values = {}
        self._seen = {}
        self._lock = threading.Lock()

    def Increment(self, request, context):
        l = self.clock.receive(request.lamport_time)
        print(f"[{self.name}] RECV Increment(counter={request.counter_id}, delta={request.delta}) L={l} (received L={request.lamport_time})")

        if self.delay_ms and not self._delayed:
            self._delayed = True
            time.sleep(self.delay_ms / 1000)

        with self._lock:
            if request.idempotency_key in self._seen:
                counter_id, value = self._seen[request.idempotency_key]
                
                l_send = self.clock.tick()
                print(f"[{self.name}] SEND IncrementReply(new_value={value}) L={l_send}")
                
                return counter_pb2.IncrementReply(
                    new_value=value, 
                    was_duplicate=True,
                    lamport_time=l_send
                )
            else:
                a = self._values.get(request.counter_id, 0)
                b = a+request.delta
                self._values[request.counter_id] = b
                self._seen[request.idempotency_key] = (
                    request.counter_id,
                    b
                )
                l_apply = self.clock.tick()
                print(f"[{self.name}] APPLY counter={request.counter_id} -> {b} L={l_apply}")

                l_send = self.clock.tick()
                print(f"[{self.name}] SEND IncrementReply(new_value={b}) L={l_send}")

                return counter_pb2.IncrementReply(
                    new_value=b,
                    was_duplicate=False,
                    lamport_time=l_send
                )
        

    def Get(self, request, context):
        l = self.clock.receive(request.lamport_time)
        print(f"[{self.name}] RECV Get(counter={request.counter_id}) L={l} (received L={request.lamport_time})")
        with self._lock:
            if request.counter_id in self._values:
                value = self._values[request.counter_id]
                l_send = self.clock.tick()
                print(f"[{self.name}] SEND GetReply(value={value}, found=True) L={l_send}")
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id], 
                    found=True,
                    lamport_time=l_send
                )
            else:
                l_send = self.clock.tick()
                print(f"[{self.name}] SEND GetReply(value=0, found=False) L={l_send}")
                return counter_pb2.GetReply(
                    value=0, 
                    found=False,
                    lamport_time=l_send
                )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--delay-ms", type=int, default=0)
    # Lab 6-7 (key-value store): the first address of --cluster is the Leaseholder.
    parser.add_argument("--cluster", default=None,
                        help="comma-separated replica addresses; enables the KeyValueStore service")
    parser.add_argument("--heartbeat-ms", type=int, default=100)
    parser.add_argument("--quiet", action="store_true", help="no per-request log of the key-value service")

    args = parser.parse_args()
    addr = f"127.0.0.1:{args.port}"

    cluster = args.cluster.split(",") if args.cluster else None
    if cluster is None and addr in DEFAULT_CLUSTER:
        cluster = DEFAULT_CLUSTER

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16 if cluster else 8))
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(args.name, args.delay_ms), server)
    if cluster:
        kv = KVServicer(addr, cluster, heartbeat_ms=args.heartbeat_ms, verbose=not args.quiet)
        kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(kv, server)
    server.add_insecure_port(addr)
    server.start()

    print(f"[{args.name}] Listening on {addr}")
    if cluster:
        role = "Leaseholder" if kv.is_leaseholder else "Follower"
        print(f"[{kv.name}] KeyValueStore enabled ({role})")
    server.wait_for_termination()


if __name__ == "__main__":
    main()