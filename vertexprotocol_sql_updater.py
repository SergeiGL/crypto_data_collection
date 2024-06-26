from time import sleep, time_ns
from datetime import datetime, timedelta
from threading import Event, Thread, RLock

import orjson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db import create_futures_db_connection, insert_or_update_futures_thread
from tg import send_telegram_error, start_telegram_worker
from wsocket import process_websocket, WebSocketApp


from sql_config import DB_CONFIG
from exchanges.vertexprotocol import VertexprotocolFuturesExchange

exchange = VertexprotocolFuturesExchange()
TOKENS_LIST = exchange.tokens

# Dictionary to store updates
update_dict = exchange.create_update_dict()

active_threads = []
stop_event = Event()
lock = RLock()
ws_app: WebSocketApp = None


def next_hour_timestamp():
    current_time = datetime.now()
    next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return int(next_hour.timestamp() * 1000)


def make_vertexprotocol_API_request(session, vertexprotocol_api_endpoint, retry_attempts: int = 5, params=None,
                                    type='get'):
    for i in range(retry_attempts):
        try:
            if type == 'get':
                response = session.get(vertexprotocol_api_endpoint, params=params, timeout=10)
            else:
                response = session.post(vertexprotocol_api_endpoint, json=params, timeout=10)

            if response.status_code == 200:
                return response
            if response.status_code == 400:
                return None
        except requests.RequestException as e:
            print(f"Request failed due to {e}. Retrying...")

        # Exponential back-off logic
        sleep(0.5 * (2 ** i))

    return None


def thread_volume24h_data(session, update_dict, stop_event):
    while not stop_event.is_set():
        data = make_vertexprotocol_API_request(session,
                                               vertexprotocol_api_endpoint='https://archive.prod.vertexprotocol.com/v2/tickers',
                                               params={'market': 'perp'}).json()
        # {   "ETH_USDC": {
        #                     "ticker_id": "ETH_USDC",         "base_currency": "ETH",
        #                     "quote_currency": "USDC",        "last_price": 1619.1,
        #                     "base_volume": 1428.32,          "quote_volume": 2310648.316391866,
        #                     "price_change_percent_24h": -1.0509394462969588
        #                 },
        # }
        with lock:
            for token in data.keys():
                element = data[token]
                if token in TOKENS_LIST:
                    update_dict[token].update({"volume24h": element['quote_volume']})

        if stop_event.wait(3600):
            break


def get_ids():
    data = make_vertexprotocol_API_request(session,
                                           vertexprotocol_api_endpoint='https://gateway.prod.vertexprotocol.com/v1/symbols',
                                           ).json()
    # [{"product_id": 0, "symbol": "USDC"},]
    key_to_id = {}
    id_to_key = {}
    for element in data:
        key_to_id[element['symbol']] = element['product_id']
        id_to_key[element['product_id']] = element['symbol']
    return key_to_id, id_to_key


def on_message(ws, message):
    data = orjson.loads(message)
    # {
    #   "status": "success",
    #   "data": {
    #     "market_prices": [
    #       {
    #         "product_id": 1,
    #         "bid_x18": "31315000000000000000000", # bid * 1e18
    #         "ask_x18": "31326000000000000000000"
    #       },
    #     ]
    #   },
    #   "request_type": "query_market_prices"
    # }

    if isinstance(data, dict):
        if "request_type" in data and data['request_type'] == 'query_market_prices' and data['status'] == "success":
            market_prices = data['data']['market_prices']
            with lock:
                for element in market_prices:
                    symbol = id_to_key[element['product_id']] + '_USDC'
                    update_dict[symbol].update({
                        'bidPrice': float(element['bid_x18']) / 1e18,
                        'askPrice': float(element['ask_x18']) / 1e18,
                        'time_bid_ask_refresh': time_ns() // 1_000_000  # time in milliseconds
                    })
        elif data['status'] != "success":  # Request error
            send_telegram_error(f"vertexprotocol_futures_sql_updater.py\nws request error'\n{data}")
    else:
        send_telegram_error(f"vertexprotocol_futures_sql_updater.py\nWrong type(data)\n{data}")


def create_id_list(tokens_to_id):
    ids_to_update = []
    for token in tokens_to_id:
        token = token.replace('_USDC', '')
        if token in key_to_id.keys():
            ids_to_update.append(key_to_id[token])
    return ids_to_update


def ws_send_bid_ask_update_signal():
    if ws_app:
        product_ids = create_id_list(TOKENS_LIST)
        message = orjson.dumps({
            "type": "market_prices",
            "product_ids": product_ids
        })

        ws_app.send(message.decode('utf-8'))


def on_open(ws):
    global ws_app
    ws_app = ws

    ws_send_bid_ask_update_signal()


def thread_funding_rate_data(session, update_dict, stop_event):
    while not stop_event.is_set():
        product_ids = create_id_list(TOKENS_LIST)
        data = make_vertexprotocol_API_request(session,
                                               vertexprotocol_api_endpoint='https://archive.prod.vertexprotocol.com/v1',
                                               params={"funding_rates": {"product_ids": product_ids}},
                                               type='post').json()
        # {
        #   "2": {
        #     "product_id": 2,
        #     "funding_rate_x18": "-697407056090986",
        #     "update_time": "1692825387"
        #   },
        # }
        timestamp_next_hour = next_hour_timestamp()
        with lock:
            for id in data.keys():
                element = data[id]
                symbol = id_to_key[element['product_id']] + '_USDC'
                update_dict[symbol].update({
                    'funding_period': 1,
                    'funding_annual_percent': float(element['funding_rate_x18']) / 1e18 * 365 * 100,
                    'time_funding_refresh': time_ns() // 1_000_000,  # time in milliseconds
                    'nextFundingTime': timestamp_next_hour,
                })
            ws_send_bid_ask_update_signal()

        if stop_event.wait(60 * 5):
            break


def thread_openInterest_data(session, update_dict, stop_event):
    while not stop_event.is_set():
        product_ids = create_id_list(TOKENS_LIST)
        param = {
            "market_snapshots": {
                "interval": {
                    "count": 1,
                    "granularity": 3600,
                    "max_time": time_ns() // 1_000_000_000,
                },
                "product_ids": product_ids
            }
        }
        data = make_vertexprotocol_API_request(session,
                                               vertexprotocol_api_endpoint='https://archive.prod.vertexprotocol.com/v1',
                                               params=param,
                                               type='post').json()

        with lock:
            data = data["snapshots"][0]["open_interests"]
            for id in data.keys():
                symbol = id_to_key[int(id)] + '_USDC'
                value = data[id]
                update_dict[symbol].update({
                    'openInterest': float(value) / 1e18,
                    'time_openInterest_refresh': time_ns() // 1_000_000})  # time in milliseconds


        if stop_event.wait(60 * 5):
            break


if __name__ == '__main__':
    start_telegram_worker(stop_event)
    CONNECTION = create_futures_db_connection(db_config=DB_CONFIG,
                                              table_name=exchange.table_name)

    session = requests.Session()
    retry_strategy = Retry(
        total=1,  # Number of retries
        backoff_factor=10,  # Delay between retries
        status_forcelist=[401, 402, 403, 500, 502, 503, 504])

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Dictionary to store id/token pairs
    key_to_id, id_to_key = get_ids()



    thread1 = Thread(target=thread_volume24h_data, args=(session, update_dict, stop_event))
    thread1.start()
    active_threads.append(thread1)

    thread2 = Thread(target=thread_funding_rate_data, args=(session, update_dict, stop_event))
    thread2.start()
    active_threads.append(thread2)

    thread3 = Thread(target=thread_openInterest_data, args=(session, update_dict, stop_event))
    thread3.start()
    active_threads.append(thread3)

    insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(
        url="wss://gateway.prod.vertexprotocol.com/v1/ws",
        on_open=on_open,
        on_message=on_message,
        ping_interval=28,
        active_threads=active_threads,
    )
