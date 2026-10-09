import threading


class LamportClock:
    def __init__(self, name):
        self.name = name          # например "client-1" или "replica-A"
        self.time = 0
        self._lock = threading.Lock()

    def tick(self):
        # своё событие: +1 под lock и вернуть новое значение
        with self._lock:
            self.time = self.time + 1
            return self.time

    def receive(self, received):
        with self._lock:
            self.time = max(self.time, received) + 1
            return self.time