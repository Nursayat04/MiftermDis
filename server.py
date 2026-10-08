import argparse
import threading
from concurrent import futures

import grpc
import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> число
        self._seen = {}     # idempotency_key -> (counter_id, итоговое значение)

    def Increment(self, request, context):
        with self._lock:
            if request.idempotency_key in self._seen:
                 counter_id, value = self._seen[request.idempotency_key]
                 return counter_pb2.IncrementReply(
                    new_value=value, 
                    was_duplicate=True
                )
            else:
                a = self._values.get(request.counter_id, 0)
                b = a+request.delta
                self._values[request.counter_id] = b
                self._seen[request.idempotency_key] = (
                    request.counter_id,
                    b
                )

                return counter_pb2.IncrementReply(
                    new_value=b,
                    was_duplicate=False
                )
            pass

    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id], 
                    found=True
                )
            else:
                return counter_pb2.GetReply(
                    value=0, 
                    found=False
                )
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(), server)
    server.add_insecure_port(f"127.0.0.1:{args.port}")
    server.start()
    print(f"[SERVER] Listening on 127.0.0.1:{args.port}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()