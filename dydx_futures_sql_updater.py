from datetime import datetime, timezone
from time import time_ns
from threading import Thread, Event, RLock

import orjson
from sortedcontainers import SortedList

from db import create_futures_db_connection, insert_or_update_futures_thread
from tg import send_telegram_error, start_telegram_worker
from wsocket import process_websocket

from exchanges.dydx import DydxFuturesExchange
from sql_config import DB_CONFIG

exchange = DydxFuturesExchange()
TOKENS_LIST = exchange.tokens

# Dictionary to store updates
update_dict = exchange.create_update_dict()

active_threads = []
stop_event = Event()


class OrderBook:
    def __init__(self):
        self.order_books = {}

    def process_initial_data(self, initial_data):
        token_id = initial_data["id"]
        self.order_books[token_id] = {"bids": SortedList(), "asks": SortedList(), 'lock': RLock()}

        for side in ['bids', 'asks']:
            for order in initial_data["contents"][side]:
                price, offset, size = float(order["price"]), int(order["offset"]), float(order["size"])
                book = self.order_books[token_id]
                with book['lock']:
                    book[side].add((price, offset, size))

    def update_order_book_and_best_bid_ask(self, update_data, update_dict):
        token_id = update_data["id"]
        new_offset = int(update_data["contents"]["offset"])
        book = self.order_books[token_id]
        with book['lock']:
            for side in ['bids', 'asks']:
                book_side = book[side]
                for price, size in update_data["contents"][side]:
                    price, size = float(price), float(size)
                    index = book_side.bisect_left((price,))

                    if index < len(book_side) and book_side[index][0] == price:
                        if new_offset > book_side[index][1]:
                            del book_side[index]
                            book_side.add((price, new_offset, size))
                    else:
                        book_side.add((price, new_offset, size))

            self.update_best_bid_ask(token_id, update_dict)

    def update_best_bid_ask(self, token_id, update_dict):
        book = self.order_books[token_id]
        best_bid = next((bid[0] for bid in reversed(book['bids']) if bid[2] > 0), None)
        best_ask = next((ask[0] for ask in book['asks'] if ask[2] > 0), None)
        update_dict[token_id].update(
            {'bidPrice': best_bid, 'askPrice': best_ask, 'time_bid_ask_refresh': time_ns() // 1_000_000})

    def clear_zero_size_low_offset_orders(self):
        for token_id, book in self.order_books.items():
            with book['lock']:
                for side in ['bids', 'asks']:
                    orders = book[side]

                    if orders:
                        median_offset = self.quickselect_median(orders)
                        book[side] = SortedList(
                            order for order in orders
                            if not (order[2] == 0 and order[1] < median_offset)
                        )

    @staticmethod
    def quickselect_median(lst):
        offsets = [item[1] for item in lst]
        n = len(offsets)

        # Sort the offsets to find the median
        offsets.sort()

        if n % 2 == 1:
            return offsets[n // 2]
        else:
            mid = n // 2
            return (offsets[mid - 1] + offsets[mid]) / 2


order_book = OrderBook()


def on_message(ws, message):
    global order_book

    data = orjson.loads(message)
    # if 'openInterest' in message:
    #     print(message)

    typ = data.get('type')
    if typ == 'connected':
        # {'type': 'connected', 'connection_id': '28764e52-41e6-4644-a345-168b4e83fab0', 'message_id': 0}
        return
    elif typ not in ['subscribed', 'channel_data']:
        send_telegram_error(f"Wrong type\n{data}")
        return

    channel = data['channel']

    if channel == 'v3_markets':
        handle_market_data(contents=data['contents'], update_dict=update_dict)
    elif channel == 'v3_orderbook':
        handle_orderbook_data(data=data, order_book=order_book)
    else:
        send_telegram_error(f"Wrong channel\n{data}")


def handle_orderbook_data(data, order_book: OrderBook):
    if data['type'] == 'subscribed':
        order_book.process_initial_data(data)
    else:
        order_book.update_order_book_and_best_bid_ask(update_data=data, update_dict=update_dict)


def handle_market_data(contents, update_dict):
    # print('handle_market_data', contents)
    if 'markets' in contents:  # if no market then the dictionary is like this: {'ETH-USD': {'indexPrice': '1962.736'}, '1INCH-USD': {'indexPrice': '0.35728'},
        for market, values in contents['markets'].items():
            # print('\t',contents)
            if values['status'] == 'ONLINE' and values['type'] == 'PERPETUAL':
                update_dict[market].update({"token": market})

                if "nextFundingRate" in values:
                    update_dict[market].update(
                        {"funding_annual_percent": float(values["nextFundingRate"]) * 876000, "funding_period": 1,
                         "time_funding_refresh": time_ns() // 1_000_000})  # * 24 * 365 * 100

                if "nextFundingAt" in values:
                    update_dict[market].update({"nextFundingTime": int(
                        datetime.fromisoformat(values["nextFundingAt"].replace("Z", "+00:00")).replace(
                            tzinfo=timezone.utc).timestamp() * 1000)})

                if "volume24H" in values:
                    update_dict[market].update({"volume24h": float(values["volume24H"])})
                if "openInterest" in values:
                    update_dict[market].update({"openInterest": float(values["openInterest"]),
                                                "time_openInterest_refresh": time_ns() // 1_000_000})

            else:
                # send_telegram_error(f"{market} not ONLINE or not PERPETUAL")
                return
    else:
        for market, values in contents.items():
            if market in update_dict:
                if "nextFundingRate" in values:
                    update_dict[market].update(
                        {"funding_annual_percent": float(values["nextFundingRate"]) * 876000, "funding_period": 1,
                         "time_funding_refresh": time_ns() // 1_000_000})  # * 24 * 365 * 100

                if "nextFundingAt" in values:
                    update_dict[market].update({"nextFundingTime": int(
                        datetime.fromisoformat(values["nextFundingAt"].replace("Z", "+00:00")).replace(
                            tzinfo=timezone.utc).timestamp() * 1000)})

                if "volume24H" in values:
                    update_dict[market].update({"volume24h": float(values["volume24H"])})

                if "openInterest" in values:
                    update_dict[market].update({"openInterest": float(values["openInterest"]),
                                                "time_openInterest_refresh": time_ns() // 1_000_000})


def clear_orders_periodically(order_book: OrderBook, stop_event: Event):
    while not stop_event.is_set():
        order_book.clear_zero_size_low_offset_orders()

        if stop_event.wait(300):
            break


def on_open(ws):
    ws.send(orjson.dumps({"type": "subscribe", "channel": "v3_markets"}))

    for token in TOKENS_LIST:
        ws.send(
            orjson.dumps({"type": "subscribe", "channel": "v3_orderbook", "id": f"{token}", "includeOffsets": True}))


if __name__ == '__main__':
    start_telegram_worker(stop_event)
    CONNECTION = create_futures_db_connection(db_config=DB_CONFIG, table_name=exchange.table_name)
    thread1 = Thread(target=clear_orders_periodically, args=(order_book, stop_event))
    thread1.start()
    active_threads.append(thread1)

    insert_or_update_futures_thread(CONNECTION, exchange.table_name, update_dict)

    process_websocket(
        url="wss://api.dydx.exchange/v3/ws",
        on_open=on_open,
        on_message=on_message,
        stop_event=stop_event,
        active_threads=active_threads,
    )
