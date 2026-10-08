import argparse
import time
import uuid

import grpc
import counter_pb2
import counter_pb2_grpc

SERVER = "127.0.0.1:50051"
TIMEOUT = 2.0
BACKOFFS = [0.2, 0.4, 0.8]


def incr(stub, counter_id, delta, key):
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key
    )
    for attempt in range(4):
        try:
            return stub.Increment(request, timeout=TIMEOUT)
        except grpc.RpcError as e:
            if e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                if attempt<3:
                    time.sleep(BACKOFFS[attempt])
                    continue
            raise


def get(stub, counter_id):
    reply = stub.Get(counter_pb2.GetRequest(counter_id=counter_id), timeout=TIMEOUT)
    return reply


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()

    channel = grpc.insecure_channel(SERVER)
    stub = counter_pb2_grpc.CounterStub(channel)

    if args.cmd == "incr":
        key = args.key or str(uuid.uuid4())   # ключ один раз, до первой попытки
        reply = incr(stub, args.counter_id, args.by, key)
        dup = "yes" if reply.was_duplicate else "no"
        print(f"OK committed value={reply.new_value} (duplicate: {dup})")
    else:
        reply = get(stub, args.counter_id)
        if reply.found:
            print(f"value={reply.value}")
        else:
            print("not found")


if __name__ == "__main__":
    main()