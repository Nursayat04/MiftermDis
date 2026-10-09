import argparse
import threading
from concurrent import futures
from clocks import LamportClock

import grpc
import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A"):
        self.name = name
        self.clock = LamportClock(name)
        self._values = {}
        self._seen = {}
        self._lock = threading.Lock()

    def Increment(self, request, context):
        l = self.clock.receive(request.lamport_time)
        print(f"[{self.name}] RECV Increment(counter={request.counter_id}, delta={request.delta}) L={l} (received L={request.lamport_time})")   
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
                # ТЕПЕРЬ безопасно используем переменную value в принте и ответе
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
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    # Передаем args.name сюда:
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(args.name), server)
    server.add_insecure_port(f"127.0.0.1:{args.port}")
    server.start()
    
    # Заодно добавил вывод имени сервера в логи, чтобы было видно, кто запустился
    print(f"[{args.name}] Listening on 127.0.0.1:{args.port}")
    server.wait_for_termination()

if __name__ == "__main__":
    main()