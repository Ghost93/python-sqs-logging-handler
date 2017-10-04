import Queue
import threading


class AsyncHandler(threading.Thread):
    def __init__(self, handler):
        threading.Thread.__init__(name='AsyncLogHandler')
        self._handler = handler
        self._queue = Queue.Queue()

        self.daemon = True
        self.start()

    def run(self):
        while True:
            record = self._queue.get(block=True)
            self._handler.emit(record)

    def emit(self, record):
        self._queue.put(record)
