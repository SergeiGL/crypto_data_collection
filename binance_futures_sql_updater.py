from time import sleep, time_ns
from threading import Event, Thread, RLock

import orjson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db import create_futures_db_connection, insert_or_update_futures_thread
from tg import send_telegram_error, start_telegram_worker
from wsocket import process_websocket

from sql_config import DB_CONFIG
from exchanges.binance import BinanceFuturesExchange


exchange = BinanceFuturesExchange()
TOKENS_LIST = exchange.tokens

# Dictionary to store updates
update_dict = exchange.create_update_dict()

active_threads = []
stop_event = Event()
lock = RLock()


def make_binance_API_request(session, binance_api_endpoint, retry_attempts: int = 5, params=None):
    for i in range(retry_attempts):
        try:

            response = session.get(binance_api_endpoint, params=params, timeout=10)

            if response.status_code == 200:
                return response
            if response.status_code == 400:
                return None
        except requests.RequestException as e:
            print(f"Request failed due to {e}. Retrying...")

        # Exponential back-off logic
        sleep(0.5 * (2 ** i))

    return None


def on_message(ws, message):
    data = orjson.loads(message)

    # print(data)
    if isinstance(data, dict):
        if "e" in data and data['e'] == 'bookTicker':
            symbol = data['s']

            with lock:
                update_dict[symbol].update({
                    'bidPrice': float(data['b']),
                    'askPrice': float(data['a']),
                    'time_bid_ask_refresh': time_ns() // 1_000_000  # time in milliseconds
                })
        elif data['result']:  # None means the subscribe message {'result': None, 'id': 2}
            send_telegram_error(f"binance_futures_sql_updater.py\nif e in data and data['e'] == 'bookTicker'\n{data}")

    elif isinstance(data, list):
        handle_mark_price_update(data, update_dict)
    else:
        send_telegram_error(f"binance_futures_sql_updater.py\nWrong type(data)\n{data}")


def handle_mark_price_update(data_initial, update_dict):
    for data in data_initial:
        symbol = data['s']

        if not (symbol in TOKENS_LIST):
            continue

        funding_annual_percent = float(data['r']) / update_dict[symbol]['funding_period'] * 876000  # *24*365*100
        next_funding_time = int(data['T'])

        with lock:
            update_dict[symbol].update({
                'funding_annual_percent': funding_annual_percent,
                'nextFundingTime': next_funding_time,
                'time_funding_refresh': time_ns() // 1_000_000  # time in milliseconds
            })


# Quote asset should be in USD to get volume in USD
def thread_volume24h_data(session, tokens_list, update_dict, stop_event):
    while not stop_event.is_set():
        responce = make_binance_API_request(session,
                                            binance_api_endpoint='https://fapi.binance.com/fapi/v1/ticker/24hr')

        data = responce.json()
        print(responce.headers['x-mbx-used-weight-1m'])
        with lock:
            for row in data:
                if row["symbol"] in tokens_list:
                    update_dict[row["symbol"]].update({"volume24h": float(row["quoteVolume"])})

        if stop_event.wait(3600):
            break


# def thread_openInterest_data(session, tokens_list, update_dict, stop_event):
#     while not stop_event.is_set():
#         responce = make_binance_API_request(session,
#                                             binance_api_endpoint='https://fapi.binance.com/fapi/v1/openInterest')
#
#         data = responce.json()
#
#         # print(data)
#         with lock:
#             for row in data:
#                 if row["symbol"] in tokens_list:
#                     update_dict[row["symbol"]].update({"openInterest": float(row["openInterest"])})
#                     update_dict[row["symbol"]].update({"time_openInterest_refresh": time_ns()//1_000_000})
#
#         if stop_event.wait(3600):
#             break

def thread_openInterest_data(session, tokens_list, update_dict, stop_event):
    while not stop_event.is_set():

        base_url = 'https://fapi.binance.com/fapi/v1/openInterest'


        for token in tokens_list:
            params = {
                'symbol': token,

            }
            response = session.get(base_url, params=params)
            data = response.json()
            with lock:
                update_dict[data["symbol"]].update({"openInterest": float(data["openInterest"])})
                update_dict[data["symbol"]].update({"time_openInterest_refresh": time_ns() // 1_000_000})

        if stop_event.wait(60*5):
            break

            # Gets funding period based on 2 last funding times


def thread_funding_period_data(session, tokens_list, update_dict, stop_event):
    while not stop_event.is_set():

        base_url = 'https://fapi.binance.com/fapi/v1/fundingRate'
        limit = 2  # Fetching the two most recent funding rates

        for token in tokens_list:
            params = {
                'symbol': token,
                'limit': limit
            }
            response = session.get(base_url, params=params)
            data = response.json()
            # print(response.headers)

            # Calculate the difference in hours between the two funding times
            funding_time_diff = (data[1]['fundingTime'] - data[0]['fundingTime']) // (
                        1000 * 3600)  # Converting to hours
            with lock:
                update_dict[token].update({'funding_period': funding_time_diff})

        if stop_event.wait(3600):
            break


def on_open(ws):
    subscribe_message = orjson.dumps({
        "method": "SUBSCRIBE",
        "params": [f"{token.lower()}@bookTicker" for token in TOKENS_LIST] + ['!markPrice@arr@1s'],
        "id": 2
    })
    ws.send(subscribe_message)


if __name__ == '__main__':
    start_telegram_worker(stop_event)
    CONNECTION = create_futures_db_connection(db_config=DB_CONFIG,
                                              table_name=exchange.table_name)

    session = requests.Session()
    retry_strategy = Retry(
        total=100,  # Number of retries
        backoff_factor=10,  # Delay between retries
        status_forcelist=[401, 402, 403, 500, 502, 503, 504])

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    thread1 = Thread(target=thread_volume24h_data, args=(session, TOKENS_LIST, update_dict, stop_event))
    thread1.start()
    active_threads.append(thread1)

    thread2 = Thread(target=thread_funding_period_data,
                     args=(session, TOKENS_LIST, update_dict, stop_event))
    thread2.start()
    active_threads.append(thread2)

    thread3 = Thread(target=thread_openInterest_data,
                     args=(session, TOKENS_LIST, update_dict, stop_event))
    thread3.start()
    active_threads.append(thread3)

    insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(url="wss://fstream.binance.com/ws",
                      on_open=on_open,
                      on_message=on_message,
                      stop_event=stop_event,
                      active_threads=active_threads,
                      )
