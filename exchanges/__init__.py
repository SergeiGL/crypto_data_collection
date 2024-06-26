from abc import ABC
from typing import List
import re


class Exchange(ABC):

    _coins: List[str] = None
    _template: str = '{coin}'
    _coin_re: str = r'^(\w+)$'
    _coin_rec = None
    table_name: str = None
    tax_table_name: str = None

    @property
    def coins(self):
        return self._coins

    @property
    def tokens(self):
        return list(map(self.coin2token, self._coins))

    def coin2token(self, coin: str) -> str:
        return self._template.format(coin=coin)

    @property
    def coin_re(self):
        if self._coin_rec is None:
            self._coin_rec = re.compile(self._coin_re)
        return self._coin_rec

    def token2coin(self, token: str) -> str:
        res = self.coin_re.search(token)
        assert res
        return res.group(1)

    def create_update_dict(self):
        return {
            token: {
                'token': token,
                'funding_annual_percent': -1,
                'nextFundingTime': -1,
                'funding_period': -1,
                'bidPrice': -1,
                'askPrice': -1,
                'volume24h': -1,
                'time_funding_refresh': -1,
                'time_bid_ask_refresh': -1,
                'openInterest': -1.,
                'time_openInterest_refresh': -1,
            }
            for token in self.tokens
        }
