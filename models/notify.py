from dataclasses import dataclass
from enum import Enum
from typing import Union


class NotifyType(Enum):
    # Price alerts only (spot or futures)
    price_alerts_only = 1

    # Funding rates alerts only
    funding_rates_alerts_only = 2

    # Funding to margin alerts
    funding_margin_rates_alerts = 3

    # Price + funding rates alerts only
    price_and_funding_rates_alerts = 4


class NotifyState(Enum):
    started = 0
    opening = 1
    opened = 2
    closing = 3
    closed = 4


@dataclass
class NotifyRule:
    tokens: Union[list[str], set[str]]
    exchanges: Union[list[str], set[str]]
    typ: NotifyType
    sx: float = None    # price spread between exchanges
    fx: float = None    # funding spread between exchanges
    mf1: float = None   # spread between funding and token margin on binance
    mf2: float = None   # spread between funding and usdt margin on binance


@dataclass
class Notify:
    token: str
    exchange1: str
    exchange2: str
    typ: NotifyType
    sx: float = None  # price spread between exchanges
    fx: float = None  # funding spread between exchanges
    mf1: float = None  # spread between funding and token margin on binance
    mf2: float = None  # spread between funding and usdt margin on binance
    state: NotifyState = None
    open_at = None      # time when last rise happen
