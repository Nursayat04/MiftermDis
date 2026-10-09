import threading
import grpc
import pytest
import time
from concurrent import futures

import counter_pb2_grpc
from server import CounterServicer
from client import incr, get
from client import incr, get, quorum_incr

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

@pytest.fixture
def replicas():
    servers, stubs, channels = [], [], []
    for name in ("replica-A", "replica-B", "replica-C"):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        counter_pb2_grpc.add_CounterServicer_to_server(CounterServicer(name), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        servers.append(server)
        channels.append(channel)
        stubs.append(counter_pb2_grpc.CounterStub(channel))
    yield servers, stubs
    for channel in channels:
        channel.close()
    for server in servers:
        server.stop(0)

def test_majority_commit_two_acks(replicas):
    servers, stubs = replicas
    
    # arrange: останавливаем третью реплику
    servers[2].stop(0).wait()
    
    # act: делаем инкремент, рассылая запрос по всем стабам
    replies = quorum_incr(stubs, "x", 1, "k-1")
    
    # assert: проверяем, что ответило ровно 2 реплики
    assert len(replies) == 2
    
    # проверяем, что обе ответившие реплики вернули правильное значение
    for reply in replies:
        assert reply.new_value == 1

def test_no_commit_below_majority(replicas):
    servers, stubs = replicas
    
    # arrange: останавливаем две реплики из трех
    servers[1].stop(0).wait()
    servers[2].stop(0).wait()
    
    # act: пытаемся сделать инкремент, рассылая запрос по всем стабам
    replies = quorum_incr(stubs, "x", 1, "k-1")
    
    # assert: проверяем, что подтвердило меньше двух реплик (кворум не собран)
    assert len(replies) < 2
    assert get(stubs[0], "x").value == 1

def test_replicas_converge(replicas):
    servers, stubs = replicas
    
    # act: делаем 10 записей через кворум с уникальными ключами
    for i in range(10):
        quorum_incr(stubs, "x", 1, f"k-{i}")
        
    # assert: проверяем, что все реплики в итоге имеют одинаковое значение 10
    for stub in stubs:
        reply = get(stub, "x")
        assert reply.value == 10

def test_retry_after_timeout_is_safe():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer("replica-A", delay_ms=2500), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = counter_pb2_grpc.CounterStub(channel)

    # act: делаем запрос, первая попытка упадет по таймауту, вторая вернет duplicate
    reply = incr(stub, "x", 1, "k-1")
    
    # ждем, чтобы первый (зависший) запрос гарантированно завершился на сервере
    time.sleep(0.5)

    # assert: проверяем, что дублирование запроса не привело к двойному инкременту
    assert reply.new_value == 1
    assert get(stub, "x").value == 1

    channel.close()
    server.stop(0)