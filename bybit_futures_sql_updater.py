import websocket
import orjson
import threading
import traceback
import sys
from ColoredOutput import ColoredOutput
from sql_config import DB_CONFIG
from tg import send_telegram_error, start_telegram_worker
from db import create_futures_db_connection, insert_or_update_futures_thread
import requests
from time import time_ns
from exchanges.bybit import BybitFuturesExchange


exchange = BybitFuturesExchange()
TOKENS_LIST = exchange.tokens
update_dict = exchange.create_update_dict()


def on_message(ws, message):
    data = orjson.loads(message)

    if 'success' == list(data.keys())[0]:
        return

    # print(data) 

    # Handling different topics
    if "tickers" == data['topic'].split('.')[0]:
        handle_tickers_update(data)
    else:
        send_telegram_error(f"BYBIT_futures_sql_updater.py\nUnknown topic\n{data}")


def handle_tickers_update(data):
    global update_dict

    item = data['data']
    symbol = item['symbol']

    # Preparing the updates in a single dictionary
    updates = {}
    current_time = time_ns() // 1_000_000  # time in milliseconds

    if 'bid1Price' in item:
        updates['bidPrice'] = float(item['bid1Price'])
        updates['time_bid_ask_refresh'] = current_time  # time in milliseconds

    if 'ask1Price' in item:
        updates['askPrice'] = float(item['ask1Price'])
        updates['time_bid_ask_refresh'] = current_time  # time in milliseconds

    if 'volume24h' in item:
        updates['volume24h'] = float(item['turnover24h'])

    if 'fundingRate' in item:
        updates['funding_annual_percent'] = float(item['fundingRate']) / update_dict[symbol]['funding_period'] * 876000
        updates['time_funding_refresh'] = current_time  # time in milliseconds

    if 'nextFundingTime' in item:
        updates['nextFundingTime'] = float(item['nextFundingTime'])
    if 'openInterest' in item:
        # print('openInterest', float(item['openInterest']))
        updates['openInterest'] = float(item['openInterest'])
        updates['time_openInterest_refresh'] = current_time  # time in milliseconds

    # Applying the updates in a single call
    update_dict[symbol].update(updates)


def on_open(ws):
    for token in TOKENS_LIST:
        # Subscribe to tickers topic
        message = {
            "op": "subscribe",
            "args": [
                f"tickers.{token}"
            ]
        }
        ws.send(orjson.dumps(message))


def update_funding_period(stop_event):
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {
        "category": "linear",
        "limit": 1
    }

    while not stop_event.is_set():

        try:
            for token in TOKENS_LIST:
                params['symbol'] = token
                response = requests.get(url, params=params)
                data = response.json()
                item = data["result"]["list"][0]
                update_dict[item["symbol"]].update({'funding_period': int(item["fundingInterval"]) // 60})

        except Exception as e:
            send_telegram_error(f"bybit_futures_sql_updater.p\nupdate_funding_period error\n{e}")

        ColoredOutput.green("FR period updated")

        if stop_event.wait(7200):  # 2 hours
            break


def on_error(ws, error):
    # Extract the most recent traceback
    tb = traceback.extract_tb(error.__traceback__)

    # Get the last traceback tuple (filename, line number, function name, text)
    filename, lineno, funcname, text = tb[0]
    error_message = f"WebSocket {ws.url} error: {error}\nError occurred at line {lineno}\nin file {filename}\n\nin func {funcname}:\n{text}\n{tb}"

    send_telegram_error(error_message)


def on_close(ws, close_status_code=None, close_msg=None):
    stop_event.set()

    for t in active_threads:
        t.join()

    ws.close()
    error_messsage = f"### WebSocket closed ###\n{ws.url}\nAll threads are closed."
    send_telegram_error(error_messsage)
    sys.exit()


# Database connection and initialization
CONNECTION = create_futures_db_connection(db_config=DB_CONFIG, table_name=exchange.table_name)
active_threads = []
stop_event = threading.Event()
start_telegram_worker(stop_event)

# Start funding period update
thread1 = threading.Thread(target=update_funding_period, args=(stop_event,))
thread1.start()
active_threads.append(thread1)

# Start database update thread
insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

# WebSocket configuration
ws = websocket.WebSocketApp("wss://stream.bybit.com/v5/public/linear",  # Update the URL as needed
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()
