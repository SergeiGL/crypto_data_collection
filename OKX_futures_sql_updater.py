import logging

import orjson

from db import create_futures_db_connection, insert_or_update_futures_thread
from tg import send_telegram_error
from wsocket import process_websocket
from time import time_ns
from sql_config import DB_CONFIG
from exchanges.okx import OkxFuturesExchange


exchange = OkxFuturesExchange()
TOKENS_LIST = exchange.tokens


# Dictionary to store updates
update_dict = exchange.create_update_dict()


def on_message(ws, message):
    data = orjson.loads(message)
    if 'arg' not in data:
        logging.warning(data)
        return
    channel = data['arg'].get('channel')
    instId = data['arg'].get('instId')

    if channel == 'funding-rate':
        if "data" in data:
            handle_funding_rate_update(data, instId)
        elif data['event'] != 'subscribe':
            send_telegram_error(f"OKX_futures_sql_updater.py\nUnexpected data\n{data}")
    
    elif channel == 'tickers':
        if "data" in data:
            handle_tickers_update(data, instId)
        elif data['event'] != 'subscribe':
            send_telegram_error(f"OKX_futures_sql_updater.py\nUnexpected data\n{data}")
    elif channel == 'open-interest':
        if "data" in data:
            handle_interest_update(data, instId)
        elif data['event'] != 'subscribe':
            send_telegram_error(f"OKX_futures_sql_updater.py\nUnexpected data\n{data}")

    else:
        send_telegram_error(f"OKX_futures_sql_updater.py\nUnknown channel\n{data}")


def handle_funding_rate_update(data, instId):

    if len(data['data']) == 1:
        item = data['data'][0]
        funding_rate = float(item['fundingRate'])
        funding_time = int(item['fundingTime'])  # funding time of a previous settlement
        next_funding_time = int(item['nextFundingTime'])
        funding_period = (next_funding_time-funding_time)//3600000
        
        update_dict[instId].update({
            'funding_annual_percent': funding_rate / funding_period * 876000,  #* 24 * 365 * 100,
            'nextFundingTime': next_funding_time,
            "funding_period" : funding_period,
            'time_funding_refresh' : time_ns()//1_000_000,
        })
    
    else:
        send_telegram_error("OKX_futures_sql_updater\nlen(data['data']) == 1 error")


def handle_tickers_update(data, instId):
    # print(data)
    for item in data['data']:
        bid_price = float(item['bidPx'])
        ask_price = float(item['askPx'])
        volume_24h = int(float(item['volCcy24h']))*bid_price # !!! 24h trading volume, with a unit of currency. 24h trading volume, with a unit of currency. If it is a derivatives contract, the value is the number of base currency. If it is SPOT/MARGIN, the value is the quantity in quote currency.

        update_dict[instId].update({
            'bidPrice': bid_price,
            'askPrice': ask_price,
            'volume24h': volume_24h,
            'time_bid_ask_refresh': time_ns()//1_000_000,
        })


def handle_interest_update(data, instId):
    # print(data)
    for item in data['data']:
        openInterest = float(item['oi'])
        update_dict[instId].update({
            'openInterest': openInterest,
            'time_openInterest_refresh': time_ns()//1_000_000,

        })



def on_open(ws):
    for token in TOKENS_LIST:
        for channel in ['funding-rate', 'tickers', "open-interest"]:
            message = {
                "op": "subscribe",
                "args": [
                    {
                        "channel": channel,
                        "instId": token
                    }
                ]
            }
            ws.send(orjson.dumps(message))


if __name__ == '__main__':

    CONNECTION = create_futures_db_connection(db_config=DB_CONFIG, table_name=exchange.table_name)

    insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(
        url="wss://ws.okx.com:8443/ws/v5/public",
        on_open=on_open,
        on_message=on_message
    )
