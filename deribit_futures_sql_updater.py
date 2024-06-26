import logging
import orjson
from sql_config import DB_CONFIG
from db import create_futures_db_connection, insert_or_update_futures_thread
from wsocket import process_websocket
from time import time_ns
from exchanges.deribit import DeribitFuturesExchange


exchange = DeribitFuturesExchange()
TOKENS_LIST = exchange.tokens

# Dictionary to store updates
update_dict = exchange.create_update_dict()

message_id = 0


def send_message(ws, method, params: dict = None):
    global message_id
    message_id += 1
    message = {
        "jsonrpc": "2.0",
        "method": method,
        "id": message_id,
    }
    if params:
        message["params"] = params
    ws.send(orjson.dumps(message))


def on_message(ws, message):
    data = orjson.loads(message)
    if 'error' in data:
        logging.error(data)
        # {'jsonrpc': '2.0', 'id': 43, 'error': {'message': 'Method not found', 'code': -32601}, 'usIn': 1702019843229880, 'usOut': 1702019843230762, 'usDiff': 882}
    elif 'id' in data:
        # it' s a subscribe message {'jsonrpc': '2.0', 'id': 42, 'result': ['ticker.BTC-PERPETUAL.raw'], 'usIn': 1701459165123251, 'usOut': 1701459165123420, 'usDiff': 169}
        logging.info(data)
    elif data.get('method') == 'heartbeat':
        # {'jsonrpc': '2.0', 'method': 'heartbeat', 'params': {'type': 'test_request'}}
        logging.info('heartbeat received')
        send_message(ws, 'public/test')
    elif data.get('method') == 'subscription':
        handle_tickers_update(data)
    else:
        logging.warning('unknown message %s', data)
        # {'jsonrpc': '2.0', 'method': 'heartbeat', 'params': {'type': 'test_request'}}


def handle_tickers_update(data):
    global update_dict

    item = data['params']['data']

    current_time = time_ns() // 1_000_000  # time in milliseconds
    # print(item['open_interest'])

    # Preparing the updates in a single dictionary
    update_dict[item['instrument_name']].update({
        "funding_annual_percent": item['current_funding'] / 8 * 876000,  # Funding period is always 8
        'funding_period': 8,
        "bidPrice": item['best_bid_price'],
        'askPrice': item['best_ask_price'],
        'volume24h': item['stats']['volume_usd'],
        'time_funding_refresh': current_time,
        "time_bid_ask_refresh": current_time,
        'openInterest': float(item['open_interest']),
        'time_openInterest_refresh': current_time
    })


def on_open(ws):
    # enable heartbeat
    send_message(ws, "public/set_heartbeat", {"interval": 60})

    # Subscribe to tickers topic
    for token in TOKENS_LIST:
        send_message(ws, "public/subscribe", {"channels": [f"ticker.{token}.raw"]})


if __name__ == '__main__':
    CONNECTION = create_futures_db_connection(db_config=DB_CONFIG,
                                              table_name=exchange.table_name)

    insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(
        url="wss://streams.deribit.com/ws/api/v2",
        on_open=on_open,
        on_message=on_message
    )
