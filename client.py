import argparse
import time
import uuid

import grpc
import counter_pb2
import counter_pb2_grpc
import kvstore_pb2 as kvpb
import kvstore_pb2_grpc as kvpbg
from clocks import LamportClock

SERVER = "127.0.0.1:50051"
TIMEOUT = 2.0
BACKOFFS = [0.2, 0.4, 0.8]
REPLICAS = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]

def incr(stub, counter_id, delta, key, clock=None):
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key
    )
    for attempt in range(4):
        if clock:
            request.lamport_time = clock.tick()
            print(f"[{clock.name}] SEND Increment(counter={counter_id}, delta={delta}) L={request.lamport_time}")
            
        try:
            reply = stub.Increment(request, timeout=TIMEOUT)
            
            if clock:
                l = clock.receive(reply.lamport_time)
                print(f"[{clock.name}] RECV IncrementReply(new_value={reply.new_value}) L={l} (received L={reply.lamport_time})")
                
            return reply
            
        except grpc.RpcError as e:
            if e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                if attempt < 3:
                    time.sleep(BACKOFFS[attempt])
                    continue
            raise


def get(stub, counter_id, clock=None):
    request = counter_pb2.GetRequest(counter_id=counter_id)
    
    if clock:
        request.lamport_time = clock.tick()
        print(f"[{clock.name}] SEND Get(counter={counter_id}) L={request.lamport_time}")
        
    reply = stub.Get(request, timeout=TIMEOUT)
    
    if clock:
        l = clock.receive(reply.lamport_time)
        print(f"[{clock.name}] RECV GetReply(value={reply.value}, found={reply.found}) L={l} (received L={reply.lamport_time})")
        
    return reply

def quorum_incr(stubs, counter_id, delta, key, clock=None):
    replies = []
    for stub in stubs:
        try:
            reply = incr(stub, counter_id, delta, key, clock)
            replies.append(reply)
        except grpc.RpcError:
            pass
    return replies


def is_committed(replies, total=len(REPLICAS)):
    # a write is committed only when a majority of replicas acknowledged it
    return len(replies) >= total // 2 + 1



# ---------------------------------------------------------------- Lab 6-7: key-value store
KV_CLUSTER = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]
KV_STRATEGIES = {"cp": kvpb.QUORUM_CP, "ap": kvpb.ALWAYS_AP}
KV_CONSISTENCY = {"lin": kvpb.LINEARIZABLE, "stale": kvpb.FOLLOWER_STALE}


def kv_connect(addr):
    channel = grpc.insecure_channel(addr)
    return channel, kvpbg.KeyValueStoreStub(channel)


def kv_put(stub, key, value, strategy=kvpb.QUORUM_CP, timestamp=0, idempotency_key=None):
    """Put with retries. The idempotency key is created once per logical operation."""
    request = kvpb.PutRequest(
        key=key, value=value, strategy=strategy, timestamp=timestamp,
        idempotency_key=idempotency_key or str(uuid.uuid4()),
    )
    for attempt in range(4):
        try:
            return stub.Put(request, timeout=TIMEOUT)
        except grpc.RpcError as error:
            retryable = error.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)
            if retryable and attempt < 3:
                time.sleep(BACKOFFS[attempt])
                continue
            raise


def kv_get(stub, key, consistency=kvpb.LINEARIZABLE, max_staleness_ms=0):
    request = kvpb.GetRequest(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)
    return stub.Get(request, timeout=TIMEOUT)


def kv_set_partition(stub, blocked_peers):
    """Failure injection: make this replica unreachable from / to the given peers."""
    return stub.SetPartition(kvpb.PartitionRequest(blocked_peers=list(blocked_peers)), timeout=TIMEOUT)


def run_kv_command(args):
    channel, stub = kv_connect(args.addr)
    try:
        if args.cmd == "kvput":
            reply = kv_put(stub, args.key, args.value, KV_STRATEGIES[args.strategy], args.ts, args.key_id)
            if reply.success:
                print(f"OK success=True acks_received={reply.acks_received}")
            else:
                print(f"FAILED acks_received={reply.acks_received} error={reply.error_message}")
        elif args.cmd == "kvget":
            reply = kv_get(stub, args.key, KV_CONSISTENCY[args.consistency], args.max_staleness_ms)
            if reply.found:
                print(f"value={reply.value} read_timestamp={reply.read_timestamp} is_stale={reply.is_stale}")
            else:
                print(f"not found (is_stale={reply.is_stale})")
        else:
            blocked = [b for b in args.blocked.split(",") if b]
            kv_set_partition(stub, blocked)
            print(f"partition on {args.addr}: blocked={blocked}")
    except grpc.RpcError as error:
        print(f"ERROR {error.code().name}: {error.details()}")
    finally:
        channel.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="client-1")
    parser.add_argument("--quorum", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    # Lab 6-7: key-value store commands (talk to the KeyValueStore service of a replica)
    p_kvput = sub.add_parser("kvput")
    p_kvput.add_argument("key")
    p_kvput.add_argument("value")
    p_kvput.add_argument("--strategy", choices=KV_STRATEGIES, default="cp")
    p_kvput.add_argument("--ts", type=int, default=0, help="timestamp (0 = server assigns)")
    p_kvput.add_argument("--key-id", default=None, help="idempotency key")
    p_kvput.add_argument("--addr", default=KV_CLUSTER[0])

    p_kvget = sub.add_parser("kvget")
    p_kvget.add_argument("key")
    p_kvget.add_argument("--consistency", choices=KV_CONSISTENCY, default="lin")
    p_kvget.add_argument("--max-staleness-ms", type=int, default=0)
    p_kvget.add_argument("--addr", default=KV_CLUSTER[0])

    p_kvpart = sub.add_parser("kvpartition", help="block the listed peers on --addr (empty = heal)")
    p_kvpart.add_argument("--blocked", default="", help="comma-separated peer addresses")
    p_kvpart.add_argument("--addr", default=KV_CLUSTER[0])

    args = parser.parse_args()

    if args.cmd in ("kvput", "kvget", "kvpartition"):
        run_kv_command(args)
        return
   
    clock = LamportClock(args.name)

    channel = grpc.insecure_channel(SERVER)
    stub = counter_pb2_grpc.CounterStub(channel)

    if args.cmd == "incr":
        key = args.key or str(uuid.uuid4())

        if args.quorum:
            stubs = [counter_pb2_grpc.CounterStub(grpc.insecure_channel(a)) for a in REPLICAS]
            replies = quorum_incr(stubs, args.counter_id, args.by, key, clock=clock)
            acks = len(replies)
            if is_committed(replies):
                dup = "yes" if replies[0].was_duplicate else "no"
                print(f"OK committed value={replies[0].new_value} (replicas acked: {acks}/3, duplicate: {dup})")
            else:
                print(f"FAILED: only {acks}/3 replicas acknowledged")
            return

        reply = incr(stub, args.counter_id, args.by, key, clock=clock)
        dup = "yes" if reply.was_duplicate else "no"
        print(f"OK committed value={reply.new_value} (duplicate: {dup})")
    else:
        reply = get(stub, args.counter_id, clock=clock)
        if reply.found:
            print(f"value={reply.value}")
        else:
            print("not found")


if __name__ == "__main__":
    main()