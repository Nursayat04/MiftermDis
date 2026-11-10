"""Key-value store service (Lab 6-7: CAP / PACELC), registered by server.py next to the counter service.

Cluster: the first address in --cluster is the Leaseholder (Replica A).

Writes
  QUORUM_CP  the receiving node coordinates: it collects votes from reachable
             peers; only if a majority (N/2 + 1) answered it applies the value
             and tells the voters to commit. Otherwise the write fails and
             nothing is applied anywhere.
  ALWAYS_AP  the receiving node applies the value locally (acks = 1) and pushes
             it to every reachable peer. Conflicts are resolved by
             Last-Write-Wins on the timestamp; a background anti-entropy loop
             repairs replicas after a partition heals.

Reads
  LINEARIZABLE    served by the Leaseholder (non-leaseholders proxy to it).
                  The Leaseholder refuses if it has lost contact with a quorum.
  FOLLOWER_STALE  served from the local state; if the replica has not heard
                  from the Leaseholder for longer than max_staleness_ms the
                  read fails with STALE_READ_EXCEEDED.
"""
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

PEER_TIMEOUT = 0.5
DEFAULT_CLUSTER = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]


def majority(n):
    return n // 2 + 1


def quorums_intersect(r, w, n):
    """A read quorum of size r and a write quorum of size w always share a node iff r + w > n."""
    return r + w > n


def now_us():
    return time.time_ns() // 1000


class KVServicer(pbg.KeyValueStoreServicer):
    def __init__(self, self_addr, cluster, heartbeat_ms=100, sync_every=5, verbose=False):
        self.addr = self_addr
        self.cluster = list(cluster)
        self.leaseholder = self.cluster[0]
        self.is_leaseholder = self_addr == self.leaseholder
        self.name = "replica-" + "ABCDEFGHIJ"[self.cluster.index(self_addr)]
        self.peers = [a for a in self.cluster if a != self_addr]
        self.n = len(self.cluster)
        self.quorum = majority(self.n)
        self.verbose = verbose

        self._lock = threading.Lock()      # protects _store, _blocked, _seen
        self._put_lock = threading.Lock()  # serialises writes coordinated by this node
        self._store = {}                   # key -> (timestamp, value)
        self._seen = {}                    # idempotency_key -> PutReply
        self._blocked = set()              # peers we are partitioned from
        self._channels = {}
        self._chan_lock = threading.Lock()

        self._heartbeat_s = heartbeat_ms / 1000
        self._lease_s = 3 * self._heartbeat_s
        self._sync_every = sync_every
        self._last_leader_contact = time.monotonic()
        self._lease_until = time.monotonic() + self._lease_s

        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(target=self._background, daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ utils
    def log(self, text):
        if self.verbose:
            print(f"[{self.name}] {text}", flush=True)

    def close(self):
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=2)
        with self._chan_lock:
            for channel, _ in self._channels.values():
                channel.close()
            self._channels.clear()

    def _stub(self, addr):
        with self._chan_lock:
            if addr not in self._channels:
                channel = grpc.insecure_channel(addr)
                self._channels[addr] = (channel, pbg.KeyValueStoreStub(channel))
            return self._channels[addr][1]

    def _is_blocked(self, addr):
        with self._lock:
            return addr in self._blocked

    def _apply(self, key, value, ts):
        """Last-Write-Wins: the higher (timestamp, value) pair is kept."""
        with self._lock:
            current = self._store.get(key)
            if current is None or (ts, value) > current:
                self._store[key] = (ts, value)
                return True
            return False

    def _replicate(self, peer, kind, key="", value="", ts=0):
        if self._is_blocked(peer):
            return False
        request = pb.ReplicateRequest(kind=kind, key=key, value=value, timestamp=ts, sender=self.addr)
        try:
            return self._stub(peer).Replicate(request, timeout=PEER_TIMEOUT).ok
        except grpc.RpcError:
            return False

    def _staleness_ms(self):
        if self.is_leaseholder:
            return 0
        return int((time.monotonic() - self._last_leader_contact) * 1000)

    def _lease_valid(self):
        return time.monotonic() < self._lease_until

    # ----------------------------------------------------------------- reads
    def Get(self, request, context):
        if request.consistency == pb.LINEARIZABLE:
            if self.is_leaseholder:
                if not self._lease_valid():
                    context.abort(grpc.StatusCode.UNAVAILABLE,
                                  "ReplicaUnavailableError: leaseholder lost contact with a quorum")
                self.log(f"GET {request.key} LINEARIZABLE (leaseholder)")
                return self._local_get(request.key, stale=False)
            return self._proxy_get(request, context)

        lag = self._staleness_ms()
        if request.max_staleness_ms and lag > request.max_staleness_ms:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"STALE_READ_EXCEEDED: replica lag {lag} ms > max_staleness_ms {request.max_staleness_ms}")
        self.log(f"GET {request.key} FOLLOWER_STALE (lag {lag} ms)")
        stale = (not self.is_leaseholder) and lag > 2 * self._heartbeat_s * 1000
        return self._local_get(request.key, stale=stale)

    def _local_get(self, key, stale):
        with self._lock:
            entry = self._store.get(key)
        if entry is None:
            return pb.GetReply(value="", found=False, read_timestamp=0, is_stale=stale)
        ts, value = entry
        return pb.GetReply(value=value, found=True, read_timestamp=ts, is_stale=stale)

    def _proxy_get(self, request, context):
        if self._is_blocked(self.leaseholder):
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          "ReplicaUnavailableError: leaseholder unreachable (partitioned)")
        self.log(f"GET {request.key} LINEARIZABLE -> proxy to leaseholder {self.leaseholder}")
        try:
            return self._stub(self.leaseholder).Get(request, timeout=2 * PEER_TIMEOUT)
        except grpc.RpcError as error:
            context.abort(error.code() if error.code() != grpc.StatusCode.DEADLINE_EXCEEDED
                          else grpc.StatusCode.UNAVAILABLE,
                          f"ReplicaUnavailableError: leaseholder unreachable ({error.details()})")

    # ---------------------------------------------------------------- writes
    def Put(self, request, context):
        with self._put_lock:
            key = request.idempotency_key
            if key:
                with self._lock:
                    cached = self._seen.get(key)
                if cached is not None:
                    self.log(f"PUT {request.key} duplicate idempotency_key={key} -> cached reply")
                    return cached

            ts = request.timestamp or now_us()
            if request.strategy == pb.ALWAYS_AP:
                reply = self._put_ap(request.key, request.value, ts)
            else:
                reply = self._put_cp(request.key, request.value, ts)

            if reply.success and key:
                with self._lock:
                    self._seen[key] = reply
            return reply

    def _put_cp(self, key, value, ts):
        voters = [peer for peer in self.peers if self._replicate(peer, pb.VOTE)]
        votes = 1 + len(voters)
        if votes < self.quorum:
            self.log(f"PUT {key} QUORUM_CP rejected: {votes}/{self.n} reachable, need {self.quorum}")
            return pb.PutReply(
                success=False, acks_received=votes,
                error_message=f"ReplicaUnavailableError: quorum lost ({votes}/{self.n} replicas reachable, need {self.quorum})")

        self._apply(key, value, ts)
        acks = 1 + sum(1 for peer in voters if self._replicate(peer, pb.COMMIT, key, value, ts))
        if acks < self.quorum:
            return pb.PutReply(success=False, acks_received=acks,
                               error_message=f"ReplicaUnavailableError: commit reached only {acks}/{self.n} replicas")
        self.log(f"PUT {key}={value} QUORUM_CP committed ({acks}/{self.n} acks, ts={ts})")
        return pb.PutReply(success=True, acks_received=acks, error_message="")

    def _put_ap(self, key, value, ts):
        self._apply(key, value, ts)
        acks = 1 + sum(1 for peer in self.peers if self._replicate(peer, pb.COMMIT, key, value, ts))
        self.log(f"PUT {key}={value} ALWAYS_AP accepted ({acks}/{self.n} acks, ts={ts})")
        return pb.PutReply(success=True, acks_received=acks, error_message="")

    # ----------------------------------------------- internal / failure injection
    def Replicate(self, request, context):
        if self._is_blocked(request.sender):
            context.abort(grpc.StatusCode.UNAVAILABLE, "partitioned")
        from_leader = request.sender == self.leaseholder
        if request.kind in (pb.COMMIT, pb.SYNC) and request.key:
            self._apply(request.key, request.value, request.timestamp)
        if from_leader and request.kind in (pb.HEARTBEAT, pb.COMMIT, pb.SYNC):
            self._last_leader_contact = time.monotonic()
        return pb.ReplicateReply(ok=True)

    def SetPartition(self, request, context):
        new_blocked = set(request.blocked_peers)
        with self._lock:
            healed = bool(self._blocked - new_blocked)
            self._blocked = new_blocked
        self.log(f"PARTITION blocked={sorted(new_blocked)}" if new_blocked else "PARTITION healed")
        if healed:
            self._wake.set()  # run anti-entropy right away
        return pb.PartitionReply(ok=True)

    # ------------------------------------------------------------ background
    def _background(self):
        tick = 0
        while not self._stop.is_set():
            woke = self._wake.wait(self._heartbeat_s)
            self._wake.clear()
            if self._stop.is_set():
                break
            tick += 1
            if self.is_leaseholder:
                acks = 1 + sum(1 for peer in self.peers if self._replicate(peer, pb.HEARTBEAT))
                if acks >= self.quorum:
                    self._lease_until = time.monotonic() + self._lease_s
            if woke or tick % self._sync_every == 0:
                self._anti_entropy()

    def _anti_entropy(self):
        with self._lock:
            snapshot = list(self._store.items())
        if not snapshot:
            return
        for peer in self.peers:
            for key, (ts, value) in snapshot:
                if not self._replicate(peer, pb.SYNC, key, value, ts):
                    break  # peer unreachable, try again on the next round
