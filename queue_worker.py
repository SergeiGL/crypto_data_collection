import atexit
import logging
from queue import Queue, Empty
from threading import Thread, Event
from typing import Callable

log = logging.getLogger('queue_worker')


class QueueWorker:
    # class properties
    stop_events = []            # events to stop all the QueueWorker instances
    complete_events = []        # events to wait for graceful shutdown after notify workers about exit

    stop_event: Event = None
    complete_event: Event = None
    on_message: Callable = None
    queue_full_warning_limit: int = 100
    _queue: Queue = None
    started: bool = False

    def __init__(self, on_message: Callable = None, stop_event: Event = None, queue_full_warning_limit: int = None):
        self.stop_event = stop_event or Event()
        self.on_message = on_message
        self.queue_full_warning_limit = queue_full_warning_limit or self.queue_full_warning_limit
        self._queue = Queue()
        self.complete_event = Event()

    def do_iteration(self, block=True, timeout=None):
        try:
            batch = [self._queue.get(block=block, timeout=timeout)]
        except Empty:
            return

        try:
            while True:
                batch.append(self._queue.get_nowait())
        except Empty:
            pass

        if len(batch) > 100:
            log.warning(f'queue size = {len(batch)}')

        for data in batch:
            try:
                args, kwargs = data
                self.on_message(*args, **kwargs)
            except:
                log.exception('error while process message')

    def process_messages(self):
        try:
            self.complete_events.append(self.complete_event)
            log.info('start queue processing')
            while not self.stop_event.is_set():
                self.do_iteration(timeout=0.1)
            self.do_iteration(block=False)
            log.info('finish queue processing')
        finally:
            self.complete_event.set()

    def put_message(self, *args, **kwargs):
        self._queue.put((args, kwargs))

    def start(self, on_message: Callable = None, stop_event: Event = None) -> Thread:
        self.started = True
        self.stop_event = stop_event or self.stop_event or Event()
        self.on_message = on_message or self.on_message
        self.stop_events.append(self.stop_event)

        assert callable(self.on_message)

        thread = Thread(target=self.process_messages, daemon=True)
        thread.start()
        return thread

    def stop(self):
        self.stop_event.set()

    def is_started(self):
        return self.started

@atexit.register
def grace_shutdown_workers():
    log.info('start notify workers about exit')
    complete_events = list(QueueWorker.complete_events)
    for e in list(QueueWorker.stop_events):
        e.set()
    log.info('start waiting workers for graceful shutdown')
    for e in complete_events:
        e.wait()
    log.info('finish waiting workers')
