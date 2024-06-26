from datetime import datetime
import logging
from itertools import combinations
from time import time, sleep
from typing import Union
from threading import Thread, Event

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from db import pymysql, read_last_table_data, read_last_tax_table_data
from models.future_data import FutureData
from models.tax_data import TaxData
from models.notify import NotifyRule, Notify, NotifyType, NotifyState

from exchanges import Exchange
from exchanges.binance import BinanceFuturesExchange, BinanceSpotExchange
from exchanges.deribit import DeribitFuturesExchange
from exchanges.bybit import BybitFuturesExchange
from exchanges.okx import OkxFuturesExchange
from exchanges.dydx import DydxFuturesExchange
from gs_parser import get_rules

from tg import send_telegram_message
from tg_bot_config import TG_CHAT_ID_MESSAGES


LOG_LEVEL = 'INFO'
REFRESH_EXPIRE_MS = 1_800_000     # 30 min
TAX_REFRESH_EXPIRE_MS = 7_200_000     # 2 hours
NOTIFICATION_TZ = 'Europe/Moscow'

try:
    from local_settings import *
except ImportError:
    pass

NOTIFICATION_TZ = pytz.timezone(NOTIFICATION_TZ)

futures_exchanges_map = {
    'binance': BinanceFuturesExchange(),
    'dydx': DydxFuturesExchange(),
    'okx': OkxFuturesExchange(),
    'bybit': BybitFuturesExchange(),
    'deribit': DeribitFuturesExchange(),
}

tax_exchanges_map = {
    'binance': BinanceSpotExchange(),
}

log = logging.getLogger('notifier')


class NotifyException(Exception):
    pass


class NotifyDataException(NotifyException):
    pass


class NotifyExchangeNotFound(NotifyDataException):
    pass


class NotifyTokenDataNotFound(NotifyDataException):
    pass


class NotifyTokenDataExpired(NotifyDataException):
    pass


ExchangesFutureDataDict = dict[str, dict[str, FutureData]]
ExchangesTaxDataDict = dict[str, dict[str, TaxData]]


class Notifier:
    _notifies: dict[str: Notify] = None
    _load_rules_thread: Thread = None
    _connection: pymysql.Connection = None
    db_config: dict = None
    data: ExchangesFutureDataDict = None
    tax_data: ExchangesTaxDataDict = None

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.data = {}
        self.tax_data = {}
        self._notifies = {}

    @property
    def connection(self):
        if not self._connection:
            self._connection = pymysql.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            db=self.db_config['db']
        )
        return self._connection

    @staticmethod
    def notify_key(notify: Notify):
        key = (notify.typ.name, notify.token, notify.exchange1, notify.exchange2)
        return key

    @staticmethod
    def update_notify(old: Notify, new: Notify):
        if not old:
            log.debug('add rule %s', new)
            return new
        updates = []
        if new.sx != old.sx:
            updates.append(f'sx {old.sx} -> {new.sx}')
            new.sx = old.sx
        if new.fx != old.fx:
            updates.append(f'fx {old.fx} -> {new.fx}')
            new.fx = old.fx
        if new.mf1 != old.mf1:
            updates.append(f'mf1 {old.mf1} -> {new.mf1}')
            new.mf1 = old.mf1
        if new.mf2 != old.mf2:
            updates.append(f'mf2 {old.mf2} -> {new.mf2}')
            new.mf2 = old.mf2
        if not old:
            log.debug('update rule %s %s', ', '.join(updates), old)
        return old

    def update_notifies(self, notifies: list[Notify]):
        notifies = {self.notify_key(notify): notify for notify in notifies}

        self._notifies = {
            key: self.update_notify(self._notifies.get(key), notify)
            for key, notify in notifies.items()
        }

    def check_notifies(self):
        total = len(self._notifies)
        log.debug('start check %s', total)
        activated = 0
        deactivated = 0
        active = 0
        for notify in self._notifies.values():
            prev_state = notify.state
            self.check_notify(notify, self.data, self.tax_data)
            if notify.state != prev_state:
                if notify.state == NotifyState.opened:
                    activated += 1
                elif prev_state == NotifyState.opened:
                    deactivated += 1
            if notify.state == NotifyState.opened:
                active += 1
        log.debug('finish check active %s (+%s -%s) / %s', active, activated, deactivated, total)

    def check_notify(self, notify: Notify, data: ExchangesFutureDataDict, tax_data: ExchangesTaxDataDict):
        key = self.notify_key(notify)
        try:
            if notify.typ == NotifyType.price_alerts_only:
                data1 = self.get_token_data(data, notify.exchange1, notify.token)
                data2 = self.get_token_data(data, notify.exchange2, notify.token)
                self.check_bid_ask_expired(notify.exchange1, notify.token, data1)
                self.check_bid_ask_expired(notify.exchange2, notify.token, data2)
                spread1 = (data1.askPrice - data2.bidPrice) / data2.bidPrice * 100
                spread2 = (data2.askPrice - data1.bidPrice) / data1.bidPrice * 100
                spread_price = min(spread1, spread2)
                if spread_price < notify.sx:
                    if spread_price == spread1:
                        buy_exchange, buy_data = notify.exchange1, data1
                        sell_exchange, sell_data = notify.exchange2, data2
                    else:
                        buy_exchange, buy_data = notify.exchange2, data2
                        sell_exchange, sell_data = notify.exchange1, data1

                    description = f'We have the spread less than {notify.sx}% = {spread_price:2f}%\n' \
                                  f'token {notify.token} exchange {buy_exchange} ask={buy_data.askPrice:2f}\n' \
                                  f'token {notify.token} exchange {sell_exchange} bid={sell_data.bidPrice:2f}'

                    return self.set_notify(notify, description)
            elif notify.typ == NotifyType.funding_rates_alerts_only:
                data1 = self.get_token_data(data, notify.exchange1, notify.token)
                data2 = self.get_token_data(data, notify.exchange2, notify.token)
                self.check_funding_expired(notify.exchange1, notify.token, data1)
                self.check_funding_expired(notify.exchange2, notify.token, data2)
                spread_funding = abs(data1.funding_annual_percent - data2.funding_annual_percent)
                if spread_funding > notify.fx:
                    if data1.funding_annual_percent > data2.funding_annual_percent:
                        buy_exchange, buy_data = notify.exchange2, data2
                        sell_exchange, sell_data = notify.exchange1, data1
                    else:
                        buy_exchange, buy_data = notify.exchange1, data1
                        sell_exchange, sell_data = notify.exchange2, data2

                    description = f'We have the funding rate spread more than {notify.fx}% = {spread_funding:2f}%\n' \
                                  f'FR token {notify.token} exchange {buy_exchange} = {buy_data.funding_annual_percent:2f}\n' \
                                  f'FR token {notify.token} exchange {sell_exchange} = {sell_data.funding_annual_percent:2f}\n'

                    return self.set_notify(notify, description)
            elif notify.typ == NotifyType.funding_margin_rates_alerts:
                token_future_data = self.get_token_data(data, notify.exchange1, notify.token)
                token_spot_tax = self.get_token_data(tax_data, notify.exchange2, notify.token)
                usdt_spot_tax = self.get_token_data(tax_data, notify.exchange2, 'USDT')
                self.check_funding_expired(notify.exchange1, notify.token, token_future_data)
                self.check_tax_expired(notify.exchange2, notify.token, token_spot_tax)
                self.check_tax_expired(notify.exchange2, notify.token, usdt_spot_tax)

                if token_future_data.funding_annual_percent < 0:
                    spread = abs(token_future_data.funding_annual_percent) - token_spot_tax.tax
                    if spread > notify.mf1:
                        description = f'***We have the margin-fut spread more than {notify.mf1}% = {spread:2f}%\n' \
                                      f'FR token {notify.token} exchange {notify.exchange1} = {token_future_data.funding_annual_percent:2f}\n' \
                                      f'BR token {notify.token} exchange {notify.exchange2} = {token_spot_tax.tax}***'
                        return self.set_notify(notify, description)
                else:
                    spread = token_future_data.funding_annual_percent - usdt_spot_tax.tax
                    if spread > notify.mf2:
                        description = f'***We have the margin-fut spread more than {notify.mf2}% = {spread:2f}%\n' \
                                      f'FR token {notify.token} exchange {notify.exchange1} = {token_future_data.funding_annual_percent:2f}\n' \
                                      f'BR USDT exchange {notify.exchange2} = {usdt_spot_tax.tax}***'
                        return self.set_notify(notify, description)
            elif notify.typ == NotifyType.price_and_funding_rates_alerts:
                data1 = self.get_token_data(data, notify.exchange1, notify.token)
                data2 = self.get_token_data(data, notify.exchange2, notify.token)
                self.check_funding_expired(notify.exchange1, notify.token, data1)
                self.check_funding_expired(notify.exchange2, notify.token, data2)
                spread_funding = abs(data1.funding_annual_percent - data2.funding_annual_percent)
                if spread_funding > notify.fx:
                    self.check_bid_ask_expired(notify.exchange1, notify.token, data1)
                    self.check_bid_ask_expired(notify.exchange2, notify.token, data2)

                    if data1.funding_annual_percent > data2.funding_annual_percent:
                        buy_exchange, buy_data = notify.exchange2, data2
                        sell_exchange, sell_data = notify.exchange1, data1
                    else:
                        buy_exchange, buy_data = notify.exchange1, data1
                        sell_exchange, sell_data = notify.exchange2, data2

                    spread_price = (buy_data.askPrice - sell_data.bidPrice) / sell_data.bidPrice * 100
                    if spread_price < notify.sx:
                        description = f'🎯 We have the funding rate spread more than {notify.fx}% = {spread_funding:2f}%\n' \
                                      f'FR token {notify.token} exchange {buy_exchange} = {buy_data.funding_annual_percent:2f}\n' \
                                      f'FR token {notify.token} exchange {sell_exchange} = {sell_data.funding_annual_percent:2f}\n' \
                                      f'and the price spread less than {notify.sx}% = {spread_price:2f}%\n' \
                                      f'token {notify.token} exchange {buy_exchange} ask={buy_data.askPrice:2f}\n' \
                                      f'token {notify.token} exchange {sell_exchange} bid={sell_data.bidPrice:2f}'
                        return self.set_notify(notify, description)

        except NotifyDataException as err:
            log.debug('%s %s', key, repr(err))

        self.reset_notify(notify)

    def reset_notify(self, notify: Notify):
        if notify.state != NotifyState.closed:
            notify.state = NotifyState.closed

    def set_notify(self, notify: Notify, description: str):
        if notify.state != NotifyState.opened:
            notify.state = NotifyState.opened
            notify.open_at = self.get_current_time()
            log.info(description)
            if TG_CHAT_ID_MESSAGES:
                dt = datetime.now().astimezone(NOTIFICATION_TZ)
                send_telegram_message(f'{dt:%H:%M:%S %d.%m.%Y} {description}')
            return True

    @staticmethod
    def get_exchange(exchange: str) -> Exchange:
        exchange_obj = futures_exchanges_map.get(exchange)
        if not exchange_obj:
            raise NotifyExchangeNotFound((exchange, ))
        return exchange_obj

    @staticmethod
    def get_token_data(data: Union[ExchangesFutureDataDict, ExchangesTaxDataDict], exchange: str, token: str) -> Union[FutureData, TaxData, None]:
        if not exchange:
            return None
        exchange_data = data.get(exchange)
        if not exchange_data:
            raise NotifyExchangeNotFound((exchange, ))
        token_data = exchange_data.get(token)
        if not token_data:
            raise NotifyTokenDataNotFound((exchange, token, ))
        return token_data

    def check_bid_ask_expired(self, exchange: str, token: str, token_data: FutureData):
        if token_data.bidPrice is None or token_data.bidPrice < 0:
            raise NotifyTokenDataExpired((exchange, token, 'bidPrice', token_data.bidPrice))
        if token_data.askPrice is None or token_data.askPrice < 0:
            raise NotifyTokenDataExpired((exchange, token, 'askPrice', token_data.askPrice))
        current_time = self.get_current_time()
        expired_refresh_time = current_time - REFRESH_EXPIRE_MS
        if token_data.time_bid_ask_refresh < expired_refresh_time:
            raise NotifyTokenDataExpired((exchange, token, 'time_bid_ask_refresh', current_time - token_data.time_bid_ask_refresh))

    def check_funding_expired(self, exchange: str, token: str, token_data: FutureData):
        if token_data.funding_annual_percent is None:
            raise NotifyTokenDataExpired((exchange, token, 'funding_annual_percent', token_data.funding_annual_percent))
        current_time = self.get_current_time()
        expired_refresh_time = current_time - REFRESH_EXPIRE_MS
        if token_data.time_funding_refresh < expired_refresh_time:
            raise NotifyTokenDataExpired((exchange, token, 'time_funding_refresh', current_time - token_data.time_funding_refresh))

    def check_tax_expired(self, exchange: str, token: str, token_data: TaxData):
        if token_data.tax is None:
            raise NotifyTokenDataExpired((exchange, token, 'tax', token_data.tax))
        current_time = self.get_current_time()
        expired_refresh_time = current_time - TAX_REFRESH_EXPIRE_MS
        if token_data.timestamp < expired_refresh_time:
            raise NotifyTokenDataExpired((exchange, token, 'tax_timestamp', current_time - token_data.timestamp))

    @staticmethod
    def get_current_time():
        return round(time() * 1000)

    def main(self):
        log.debug('start iteration')
        try:
            rules = get_rules()
            notifies = rules_to_notifies(rules)
            self.update_notifies(notifies)
        except Exchange as err:
            log.exception('Error while update notify rules')
            send_telegram_message(f'Error while update notify rules {str(err)}')

        self.reload_data()
        self.check_notifies()

    def reload_data(self):
        log.debug('start reload')
        try:
            for exchange, exchange_obj in futures_exchanges_map.items():
                self.data.setdefault(exchange, {}).update(read_last_table_data(self.connection, exchange_obj))
            for exchange, exchange_obj in tax_exchanges_map.items():
                self.tax_data.setdefault(exchange, {}).update(read_last_tax_table_data(self.connection, exchange_obj))
            log.debug('finish reload')
        except:
            log.exception('reload failed')


def rules_to_notifies(rules: list[NotifyRule]):
    res = []
    for rule in rules:
        if rule.typ in (NotifyType.price_alerts_only, NotifyType.funding_rates_alerts_only, NotifyType.price_and_funding_rates_alerts):
            res += [
                Notify(token=token, exchange1=exchange1, exchange2=exchange2, typ=rule.typ, sx=rule.sx, fx=rule.fx)
                for token in set(rule.tokens)
                for exchange1, exchange2 in combinations(rule.exchanges, 2)
                if token in futures_exchanges_map.get(exchange1).coins
                and token in futures_exchanges_map.get(exchange2).coins
            ]
        elif rule.typ == NotifyType.funding_margin_rates_alerts:
            res += [
                Notify(token=token, exchange1=exchange1, exchange2='binance', typ=rule.typ, mf1=rule.mf1, mf2=rule.mf2)
                for token in set(rule.tokens)
                for exchange1 in rule.exchanges
                if token in futures_exchanges_map.get(exchange1).coins
                and token in tax_exchanges_map.get('binance').coins
            ]
    return res


def setup_logging():
    logging.basicConfig(level=LOG_LEVEL, force=True, format='%(asctime)s %(levelname)s: %(message)s')


if __name__ == '__main__':
    setup_logging()

    from sql_config import DB_CONFIG

    n = Notifier(db_config=DB_CONFIG)

    n.main()

    scheduler = BackgroundScheduler()
    scheduler.add_job(n.main, trigger='cron', minute='*/1', max_instances=1, coalesce=True)
    scheduler.start()

    stop_event = Event()
    while not stop_event.is_set():
        stop_event.wait(60)

    scheduler.shutdown()
