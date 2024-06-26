from dataclasses import dataclass


@dataclass
class FutureData:
    token: str
    funding_annual_percent: float
    nextFundingTime: int
    funding_period: int
    bidPrice: float
    askPrice: float
    volume_24h: float
    time_funding_refresh: int
    time_bid_ask_refresh: int
    time_insert: int
    openInterest: float
    time_openInterest_refresh: int
