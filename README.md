# Replicated Counter Service

gRPC-сервис счётчика с дедупликацией по idempotency key, логическими часами Lamport и записью с подтверждением большинства (2 из 3 реплик).

## 1. Установка

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

Linux / macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
pip install grpcio grpcio-tools pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

(Файлы `counter_pb2.py` и `counter_pb2_grpc.py` уже лежат в репозитории; последняя команда лишь пересоздаёт их.)

## 2. Одна реплика

Терминал 1:

```powershell
python server.py --port 50051 --name replica-A
```

Терминал 2:

```powershell
python client.py incr likes:post-42 --by 5
python client.py get likes:post-42
python client.py incr likes:post-42 --by 5 --key abc
python client.py incr likes:post-42 --by 5 --key abc
```

Ожидание: после первой пары `value=5`, четвёртая команда печатает `duplicate: yes`, значение остаётся 10.

Флаги клиента `--name` и `--quorum` пишутся **до** подкоманды (`incr` или `get`).

## 3. Три реплики

В трёх разных терминалах:

```powershell
python server.py --port 50051 --name replica-A
python server.py --port 50052 --name replica-B
python server.py --port 50053 --name replica-C
```

В четвёртом терминале:

```powershell
python client.py --quorum incr likes:post-42 --by 1
```

Ожидание: `OK committed value=1 (replicas acked: 3/3, duplicate: no)`. Если остановить одну реплику (Ctrl+C) и повторить команду, будет `acked: 2/3`. Чтение `get` идёт только с реплики на порту 50051.

## 4. Сценарий B2 (три клиента, логи Lamport)

Запустить одну реплику (раздел 2), затем:

```powershell
python scenario.py
```

Сохранённая трасса и диаграмма лежат в `logs/`.

## 5. Тесты

```powershell
python -m pytest -v
```

Запускает тесты из `tests/test_counter.py` (8 тестов, включая кворум и конкурентность) и `tests/test_failures.py` (3 сценария отказов). Серверы тесты поднимают сами на свободных портах.

Только сценарии отказов:

```powershell
python -m pytest tests/test_failures.py -v
```

Имитация задержки сервера (для ручной проверки таймаута и повтора):

```powershell
python server.py --port 50051 --name replica-A --delay-ms 2500
```

Задержка срабатывает на первый запрос.

## 6. Бенчмарк

```powershell
python tests/perf_benchmark.py
```

Печатает таблицу: медиана и p95 для одной реплики и для кворума при 1 и 16 клиентах (по 2000 запросов).

## Структура

| Файл | Назначение |
|---|---|
| `counter.proto` | интерфейс gRPC |
| `server.py` | реплика (флаги `--port`, `--name`, `--delay-ms`) |
| `client.py` | подкоманды `incr` и `get`, повторы, кворумная запись (`--quorum`) |
| `clocks.py` | часы Lamport |
| `scenario.py` | сценарий B2 с тремя клиентами |
| `tests/` | тесты, тесты отказов, бенчмарк |
| `logs/` | трасса сценария, логи отказов, результаты бенчмарка |