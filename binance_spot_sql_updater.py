from time import sleep, time_ns
from threading import Event, Thread, RLock

import orjson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


from db import create_spot_db_connection, insert_or_update_spot_thread
from tg import send_telegram_error, start_telegram_worker
from wsocket import process_websocket

from sql_config import DB_CONFIG
from exchanges.binance import BinanceSpotExchange


exchange = BinanceSpotExchange()
TOKENS_LIST = exchange.tokens

# Dictionary to store updates
update_dict = exchange.create_update_dict()

active_threads = []
stop_event = Event()
lock = RLock()


def make_binance_API_request(session, binance_api_endpoint, retry_attempts: int = 5, params = None):
    for i in range(retry_attempts):
        try:
            
            response = session.get(binance_api_endpoint, params=params, timeout=10)

            if response.status_code == 200:
                return response.json()
            if response.status_code == 400:
                send_telegram_error(response.text)
                return None
        except requests.RequestException as e:
            print(f"Request failed due to {e}. Retrying...")

        # Exponential back-off logic
        sleep(0.5 * (2 ** i))

    return None


# Quote asset should be in USD to get volume in USD
def thread_fetch_volume24h_data(session, tokens_list, update_dict, stop_event):
    while not stop_event.is_set():
        params = {"symbols": orjson.dumps(tokens_list), 'type': 'MINI'}

        data = make_binance_API_request(session, params=params, binance_api_endpoint='https://api.binance.com/api/v3/ticker/24hr')

        if not data:
            return

        if len(data) != len(tokens_list):
            send_telegram_error(f"binance_spot_sql_updater.py\nif len(data) != len(tokens_list)\n{data}\n{tokens_list}")
        
        with lock:
            for row in data:
                update_dict[row["symbol"]].update({
                    "volume24h": float(row["quoteVolume"])})

        if stop_event.wait(3600):
            break


def on_message(ws, message):

    data = orjson.loads(message)

    if "s" in data:
        symbol = data['s']
        with lock:
            update_dict[symbol].update({
                'bidPrice': float(data['b']),
                'askPrice': float(data['a']),
                'time': time_ns()//1_000_000 #time in milliseconds
                })

    elif data['result']: # None means the subscribe message {'result': None, 'id': 2}
        send_telegram_error(f"binance_spot_sql_updater.py\nif s in data\n{data}")


def on_open(ws):
    subscribe_message = orjson.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{token.lower()}@bookTicker" for token in TOKENS_LIST] ,
            "id": 1
        })
    ws.send(subscribe_message)


if __name__ == '__main__':
    start_telegram_worker(stop_event)
    CONNECTION = create_spot_db_connection(db_config=DB_CONFIG, table_name=exchange.table_name)

    # Start the volume data fetching thread
    session = requests.Session()
    retry_strategy = Retry(
        total=100,  # Number of retries
        backoff_factor=10,  # Delay between retries
        status_forcelist=[401,402,403, 500, 502, 503, 504])

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    thread1 = Thread(target=thread_fetch_volume24h_data, args=(session, TOKENS_LIST, update_dict, stop_event))
    thread1.start()
    active_threads.append(thread1)

    insert_or_update_spot_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(url="wss://stream.binance.com:9443/ws",
                      on_open=on_open,
                      on_message=on_message,
                      stop_event=stop_event,
                      active_threads=active_threads,)
