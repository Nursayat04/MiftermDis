"""Lab 6-7: quorum evaluation and network-partition tests (CP vs AP).

A cluster of three replicas runs inside the test process. A network partition
is simulated with the SetPartition failure-injection RPC: each replica refuses
to talk to the peers on the other side.

Partition used in the tests:  DC-1 = {A, B} (majority)   DC-2 = {C} (minority)
"""
import time
from concurrent import futures
from itertools import combinations

import grpc
import pytest

import client
import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg
from kvstore_service import KVServicer, majority, quorums_intersect

A, B, C = 0, 1, 2
DC1, DC2 = [A, B], [C]


class Cluster:
    def __init__(self, heartbeat_ms=100):
        self.servers, ports = [], []
        for _ in range(3):
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
            ports.append(server.add_insecure_port("127.0.0.1:0"))
            self.servers.append(server)
        self.addrs = [f"127.0.0.1:{port}" for port in ports]
        self.servicers = []
        for server, addr in zip(self.servers, self.addrs):
            servicer = KVServicer(addr, self.addrs, heartbeat_ms=heartbeat_ms)
            pbg.add_KeyValueStoreServicer_to_server(servicer, server)
            server.start()
            self.servicers.append(servicer)
        self.channels = [grpc.insecure_channel(addr) for addr in self.addrs]
        self.stubs = [pbg.KeyValueStoreStub(channel) for channel in self.channels]

    def partition(self, groups):
        """Cut every link between different groups (e.g. [[A, B], [C]])."""
        for index, stub in enumerate(self.stubs):
            group = next(g for g in groups if index in g)
            blocked = [self.addrs[j] for j in range(3) if j not in group]
            client.kv_set_partition(stub, blocked)

    def heal(self):
        self.partition([[A, B, C]])

    def close(self):
        for servicer in self.servicers:
            servicer.close()
        for channel in self.channels:
            channel.close()
        for server in self.servers:
            server.stop(0)


def wait_until(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if condition():
                return True
        except grpc.RpcError:
            pass
        time.sleep(0.05)
    return False


@pytest.fixture
def cluster():
    c = Cluster()
    # wait until the leaseholder has confirmed its lease (first heartbeat round)
    assert wait_until(lambda: client.kv_get(c.stubs[A], "ready", pb.LINEARIZABLE) is not None)
    yield c
    c.close()


def local(stub, key):
    return client.kv_get(stub, key, pb.FOLLOWER_STALE)


# ------------------------------------------------------------ quorum arithmetic
def test_majority_of_three_is_two():
    assert majority(3) == 2
    assert majority(5) == 3


@pytest.mark.parametrize("n,r,w", [
    (3, 1, 1), (3, 1, 2), (3, 2, 1), (3, 2, 2), (3, 1, 3), (3, 3, 1), (5, 3, 3), (5, 2, 3), (5, 2, 4),
])
def test_quorum_intersection_matches_r_plus_w_gt_n(n, r, w):
    """Brute force over every possible read set and write set."""
    nodes = range(n)
    always_overlap = all(set(write) & set(read)
                         for write in combinations(nodes, w)
                         for read in combinations(nodes, r))
    assert always_overlap == quorums_intersect(r, w, n)


# --------------------------------------------------------------- CP (strict quorum)
def test_cp_write_succeeds_on_all_healthy_nodes(cluster):
    reply = client.kv_put(cluster.stubs[B], "k", "v1", pb.QUORUM_CP)
    assert reply.success is True
    assert reply.acks_received == 3


def test_cp_write_to_majority_partition_succeeds(cluster):
    cluster.partition([DC1, DC2])
    reply = client.kv_put(cluster.stubs[A], "k", "v1", pb.QUORUM_CP)
    assert reply.success is True
    assert reply.acks_received == 2
    reply_b = client.kv_put(cluster.stubs[B], "k2", "v2", pb.QUORUM_CP)
    assert reply_b.success is True and reply_b.acks_received == 2


def test_cp_write_to_minority_partition_is_rejected(cluster):
    cluster.partition([DC1, DC2])
    reply = client.kv_put(cluster.stubs[C], "k", "lost", pb.QUORUM_CP)
    assert reply.success is False
    assert reply.acks_received == 1
    assert "ReplicaUnavailableError" in reply.error_message
    # nothing was applied anywhere
    assert local(cluster.stubs[C], "k").found is False
    assert local(cluster.stubs[A], "k").found is False


# ------------------------------------------------------------ AP (sloppy quorum + LWW)
def test_ap_write_to_minority_partition_is_accepted(cluster):
    cluster.partition([DC1, DC2])
    reply = client.kv_put(cluster.stubs[C], "k", "kept", pb.ALWAYS_AP, timestamp=100)
    assert reply.success is True
    assert reply.acks_received == 1
    assert local(cluster.stubs[C], "k").value == "kept"
    assert local(cluster.stubs[A], "k").found is False  # not replicated across the partition


def test_ap_divergent_writes_converge_by_lww_after_heal(cluster):
    cluster.partition([DC1, DC2])
    r1 = client.kv_put(cluster.stubs[A], "color", "blue", pb.ALWAYS_AP, timestamp=100)  # DC-1
    r2 = client.kv_put(cluster.stubs[C], "color", "red", pb.ALWAYS_AP, timestamp=200)   # DC-2
    assert r1.success and r2.success
    assert local(cluster.stubs[A], "color").value == "blue"   # divergent
    assert local(cluster.stubs[C], "color").value == "red"

    cluster.heal()
    assert wait_until(lambda: all(local(s, "color").value == "red" for s in cluster.stubs))
    reply = client.kv_get(cluster.stubs[B], "color", pb.LINEARIZABLE)
    assert reply.value == "red" and reply.read_timestamp == 200


def test_lww_decides_by_timestamp_not_by_arrival_order(cluster):
    cluster.partition([DC1, DC2])
    client.kv_put(cluster.stubs[A], "color", "newer", pb.ALWAYS_AP, timestamp=300)  # DC-1, higher ts
    client.kv_put(cluster.stubs[C], "color", "older", pb.ALWAYS_AP, timestamp=200)  # DC-2, lower ts
    cluster.heal()
    assert wait_until(lambda: all(local(s, "color").value == "newer" for s in cluster.stubs))


# ------------------------------------------------------------------ read paths
def test_linearizable_read_through_follower_sees_latest_cp_write(cluster):
    client.kv_put(cluster.stubs[A], "k", "v1", pb.QUORUM_CP)
    client.kv_put(cluster.stubs[A], "k", "v2", pb.QUORUM_CP)
    reply = client.kv_get(cluster.stubs[C], "k", pb.LINEARIZABLE)  # proxied to the leaseholder
    assert reply.value == "v2"
    assert reply.is_stale is False


def test_r_plus_w_not_greater_than_n_allows_stale_read(cluster):
    """W = 2 (A, B) and a local read R = 1 (C): 2 + 1 = 3, not > 3 -> stale read possible."""
    cluster.partition([DC1, DC2])
    write = client.kv_put(cluster.stubs[A], "k", "v1", pb.QUORUM_CP)
    assert write.success and write.acks_received == 2
    stale = client.kv_get(cluster.stubs[C], "k", pb.FOLLOWER_STALE, max_staleness_ms=5000)
    assert stale.found is False                                   # stale: missed the write
    fresh = client.kv_get(cluster.stubs[A], "k", pb.LINEARIZABLE)    # leaseholder is in every write quorum
    assert fresh.value == "v1"


def test_linearizable_read_unavailable_but_follower_read_available_in_partition(cluster):
    client.kv_put(cluster.stubs[A], "k", "v1", pb.QUORUM_CP)
    cluster.partition([DC1, DC2])
    with pytest.raises(grpc.RpcError) as error:
        client.kv_get(cluster.stubs[C], "k", pb.LINEARIZABLE)
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE
    reply = client.kv_get(cluster.stubs[C], "k", pb.FOLLOWER_STALE, max_staleness_ms=5000)
    assert reply.found and reply.value == "v1"


def test_follower_read_beyond_max_staleness_is_rejected(cluster):
    cluster.partition([DC1, DC2])
    time.sleep(0.5)  # C has not heard from the leaseholder for ~500 ms
    with pytest.raises(grpc.RpcError) as error:
        client.kv_get(cluster.stubs[C], "k", pb.FOLLOWER_STALE, max_staleness_ms=100)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "STALE_READ_EXCEEDED" in error.value.details()


# -------------------------------------------------------------------- idempotency
def test_duplicate_idempotency_key_is_not_applied_twice(cluster):
    first = client.kv_put(cluster.stubs[A], "k", "first", pb.QUORUM_CP, idempotency_key="op-1")
    second = client.kv_put(cluster.stubs[A], "k", "second", pb.QUORUM_CP, idempotency_key="op-1")
    assert first.success and second.success
    assert client.kv_get(cluster.stubs[A], "k", pb.LINEARIZABLE).value == "first"
