import logging
from threading import Event, Thread
from traceback import format_exception
from typing import Callable, List

from websocket import WebSocketApp, WebSocketBadStatusException

from tg import send_telegram_error
from queue_worker import QueueWorker


_stop_event: Event = None
_active_threads: List[Thread] = None


def default_on_error(ws, error):
    if isinstance(error, WebSocketBadStatusException):
        exc = str(error)
    else:
        exc = ''.join(format_exception(error))
    error_message = f"WebSocket {ws.url} error:\n{exc}"
    send_telegram_error(error_message)


def default_on_close(ws: WebSocketApp, close_status_code=None, close_msg=None):
    if _stop_event:
        _stop_event.set()

    for t in _active_threads or []:
        t.join(timeout=5.0)

    error_messsage = f"### WebSocket closed ###\n{ws.url}\nAll threads are closed. {close_status_code} {close_msg}"

    send_telegram_error(error_messsage)



def proccess_messages(on_message: Callable, stop_event):
    while not stop_event.is_set():
        try:
            data = q.get(timeout=0.1)
        except Empty:
            data = None
        if not data:
            continue
        batch = [data]
        while not stop_event.is_set():
            try:
                data = q.get_nowait()
            except Empty:
                break
            batch.append(data)

        if len(batch) > 100:
            logging.info(f'queue size = {len(batch)}')
        for data in batch:
            on_message(None, data)


def process_websocket(url: str, on_open: Callable = None, on_message: Callable = None, on_error: Callable = None,
                      on_close: Callable = None, stop_event: Event = None, active_threads: List[Thread] = None,
                      use_queue: bool = False):
    global _stop_event, _active_threads

    _stop_event = stop_event or Event()
    _active_threads = active_threads or []

    if use_queue:
        worker = QueueWorker(on_message=on_message, stop_event=_stop_event)
        _active_threads.append(worker.start())

        def on_message(ws, message):
            worker.put_message(ws, message)

    ws = WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error or default_on_error,
        on_close=on_close or default_on_close,
    )
    ws.run_forever()
