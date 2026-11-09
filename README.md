## 1. Installation
Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## 2. One replic

Terminal 1:

```powershell
python server.py --port 50051 --name replica-A
```

Terminal 2:

```powershell
python client.py incr likes:post-42 --by 1
python client.py get likes:post-42
```

## 3. Three replics

In three different terminals:
```powershell
python server.py --port 50051 --name replica-A
python server.py --port 50052 --name replica-B
python server.py --port 50053 --name replica-C
```

In the fourth terminal:
```powershell
python client.py --quorum incr likes:post-42 --by 1
python client.py --quorum incr likes:post-42 --by 1
python client.py --quorum incr likes:post-42 --by 1
python client.py get likes:post-42
```

## 4. Shenary B2 (three Clents, Logie Lamport)

Run one replica (section 2), then:
```powershell
python scenario.py
```

## 5. Tests

```powershell
python -m pytest -v
```

Bounce scenarios only:

```powershell
python -m pytest tests/test_failures.py -v
```

```powershell
python server.py --port 50051 --name replica-A --delay-ms 2500
```

## 6. Benchmark

```powershell
python tests/perf_benchmark.py
```
## 7
python client.py --quorum incr likes:post-42 --by 1 --key demo-1
python client.py --quorum incr likes:post-42 --by 1 --key demo-1
python client.py get likes:post-42

## 8
python server.py --port 50051 --name replica-A --delay-ms 2500

python client.py incr likes:timeout --by 1
python client.py get likes:timeout