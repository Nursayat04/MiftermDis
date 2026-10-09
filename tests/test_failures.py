import os
import socket
import subprocess
import sys
import threading

import grpc
import pytest
import time
import counter_pb2_grpc
from client import quorum_incr, incr, get

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_replica(name, delay_ms=0):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "server.py", "--port", str(port), "--name", name,
         "--delay-ms", str(delay_ms)],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=10)
    return proc, counter_pb2_grpc.CounterStub(channel), channel


@pytest.fixture
def cluster():
    items = [start_replica(n) for n in ("replica-A", "replica-B", "replica-C")]
    yield items
    for proc, stub, channel in items:
        channel.close()
        proc.kill()
        proc.wait()

def test_replica_crash_mid_request(cluster):
    stubs = [item[1] for item in cluster]
    result = {}

    def writer():
        result["replies"] = quorum_incr(stubs, "x", 1, "k-1")

    t = threading.Thread(target=writer)
    t.start()
    cluster[2][0].kill()
    t.join()

    assert "replies" in result
    assert len(result["replies"]) >= 2

def test_request_duplication(cluster):
    stub = cluster[0][1]

    first = incr(stub, "x", 5, "k-1")
    second = incr(stub, "x", 5, "k-1")

    assert first.was_duplicate is False
    assert second.was_duplicate is True
    assert second.new_value == 5
    assert get(stub, "x").value == 5

def test_induced_timeout_with_retry():
    proc, stub, channel = start_replica("replica-A", delay_ms=2500)
    try:
        reply = incr(stub, "x", 1, "k-1")
        time.sleep(0.5)

        assert reply.new_value == 1
        assert get(stub, "x").value == 1
    finally:
        channel.close()
        proc.kill()
        proc.wait()