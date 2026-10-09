import threading
import uuid

import grpc
import counter_pb2_grpc
from clocks import LamportClock
from client import incr, get, SERVER


def client_1(stub):
    clock = LamportClock("client-1")
    for _ in range(2):
        incr(stub, "x", 1, str(uuid.uuid4()), clock=clock)

def client_2(stub):
    clock = LamportClock("client-2")
    for _ in range(2):
        incr(stub, "y", 1, str(uuid.uuid4()), clock=clock)


def client_3(stub):
    clock = LamportClock("client-3")
    get(stub, "x", clock=clock)
    get(stub, "y", clock=clock)
    get(stub, "x", clock=clock)
    get(stub, "y", clock=clock)


def main():
    channel = grpc.insecure_channel(SERVER)
    stub = counter_pb2_grpc.CounterStub(channel)

    threads = [
        threading.Thread(target=client_1, args=(stub,)),
        threading.Thread(target=client_2, args=(stub,)),
        threading.Thread(target=client_3, args=(stub,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()