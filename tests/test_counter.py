import threading
import grpc
import pytest
from concurrent import futures

import counter_pb2_grpc
from server import CounterServicer
from client import incr, get


@pytest.fixture
def stub():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    yield counter_pb2_grpc.CounterStub(channel)
    channel.close()
    server.stop(0)

def test_increment_applies_delta(stub):
    reply = incr(stub, "x", 5, "k-1")
    assert reply.new_value == 5
    assert get(stub, "x").value == 5

def test_duplicate_key_not_reapplied(stub):
    reply1 = incr(stub, "x", 5, "k-1")
    assert reply1.new_value == 5
    assert reply1.was_duplicate is False

    # Второй вызов с тем же ключом идемпотентности
    reply2 = incr(stub, "x", 5, "k-1")
    assert reply2.new_value == 5
    assert reply2.was_duplicate is True


def test_get_missing_counter(stub):
    reply = get(stub, "no-such")
    assert reply.found is False

def test_concurrent_increments_exact(stub):
    def worker(thread_id):
        for i in range(1000):
            incr(stub, "x", 1, f"t{thread_id}-{i}")

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert get(stub, "x").value == 2000