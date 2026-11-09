import threading


class LamportClock:
    def __init__(self, name):
        self.name = name         
        self.time = 0
        self._lock = threading.Lock()

   def tick(self):
    self

    def receive(self, received):
        with self._lock:
            self.time = max(self.time, received) + 1
            return self.time
        