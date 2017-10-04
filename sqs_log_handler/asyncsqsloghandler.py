import Queue
import threading
import time

from .sqsloghandler import BatchSQSHandler


class AsyncSQSHandler(threading.Thread):
    SQS_MAX_BATCH_SIZE = 10
    DEFAULT_TIMEOUT_IN_SEC = 5

    def __init__(self, queue_name, aws_key_id=None, secret_key=None, global_extra=None, region_name='eu-west-1'):
        threading.Thread.__init__(self, name='AsyncBatchSqsLogHandler')

        self._handler = BatchSQSHandler(queue=queue_name, aws_key_id=aws_key_id, secret_key=secret_key,
                                        global_extra=global_extra, region_name=region_name)
        self._queue = Queue.Queue()

        self.daemon = True
        self.start()

    def run(self):
        while True:
            count = 0
            records = []
            while count < self.SQS_MAX_BATCH_SIZE and not self._queue.empty():
                try:
                    record = self._queue.get(block=True, timeout=self.DEFAULT_TIMEOUT_IN_SEC)
                    records.append(record)
                except Queue.Empty:
                    break
            if len(records) > 0:
                self._handler.emit(records)

    def emit(self, record):
        self._queue.put(record)

    def sqs_batch_appender(self):
        return self._handler

    def setFormatter(self, fmt):
        self._handler.setFormatter(fmt)

    def format(self, record):
        self._handler.format(record)

    @property
    def level(self):
        return self._handler.level

    def setLevel(self, level):
        self._handler.setLevel(level)

    def handle(self, record):
        self._handler.handle(record)

    def get_name(self):
        return self._handler.name

    def flush(self):
        while not self._queue.empty():
            time.sleep(2)
