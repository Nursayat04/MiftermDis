import argparse
import time
import uuid

import grpc
import counter_pb2
import counter_pb2_grpc
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

def main():
    parser = argparse.ArgumentParser()
    # Добавлен флаг --name
    parser.add_argument("--name", default="client-1")
    parser.add_argument("--quorum", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()

    # Создаем часы
    clock = LamportClock(args.name)

    channel = grpc.insecure_channel(SERVER)
    stub = counter_pb2_grpc.CounterStub(channel)

    if args.cmd == "incr":
        key = args.key or str(uuid.uuid4())   # ключ один раз, до первой попытки

        if args.quorum:
            stubs = [counter_pb2_grpc.CounterStub(grpc.insecure_channel(a)) for a in REPLICAS]
            replies = quorum_incr(stubs, args.counter_id, args.by, key, clock=clock)
            acks = len(replies)
            if acks >= 2:
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