from . import Exchange


class BinanceFuturesExchange(Exchange):
    table_name = "Binance_fut_data"
    _template = '{coin}USDT'
    _coin_re = r'^(\w+)USDT$'
    _coins = ['XRP', 'LTC', 'FLM', 'SLP', 'ETC', 'MAGIC', 'NEAR', 'ADA', 'YFI', 'TIA', 'MANA', 'FIL', 'MINA', 'BTC',
              'REN', 'GALA', 'COMP', 'BSV', 'STX', 'ZRX', 'PEOPLE', 'OP', 'IOST', 'RDNT', 'GRT', 'SOL', 'FRONT',
              'ALPHA', 'SAND', 'UNI', 'IMX', 'PYTH', 'API3', 'MKR', 'HBAR', 'AVAX', 'WOO', 'AGIX', 'DYDX', 'INJ', 'ANT',
              'FET', 'BAT', 'ARB', 'RVN', 'GMX', 'FTM', 'THETA', 'CFX', 'EGLD', 'RNDR', 'AUCTION', 'BNT', 'BADGER',
              'ETH', 'APT', 'CHZ', 'BAND', 'ATOM', 'SSV', 'BNB', 'MEME', 'BAL', 'KSM', 'USDC', 'AAVE', 'TRB', 'IOTA',
              'OMG', 'ZIL', 'APE', 'BIGTIME', 'LDO', 'MASK', 'WAXP', 'KNC', 'LINK', 'SUSHI', 'LRC', 'DOT', 'FLOW',
              'ORBS', 'RSR', 'SUI', 'GMT', 'MATIC', 'USTC', 'ALGO', 'BLUR', 'BCH', 'DOGE', 'EOS', 'AXS', 'CELO', 'ICP',
              'XTZ', 'ENS', 'WAVES', 'ETHW', 'STORJ', '1INCH', 'YGG', 'TRX', 'XLM', 'WLD', 'KLAY', 'ACE', 'PERP', 'AR',
              'UMA', 'ONT', 'SNX', 'GAS', 'ORDI', 'LPT', 'CRV', 'QTUM', 'AGLD', 'BICO', 'ID', 'NEO']


class BinanceSpotExchange(Exchange):
    table_name = "Binance_spot_data"
    tax_table_name = "Binance_tax_margin"
    _template = '{coin}USDT'
    _coin_re = r'^(\w+)USDT$'
    _coins = ['BTC', 'ETH', 'SOL', 'ARB', 'XRP', 'LINK', 'CRV', 'DOGE', 'MATIC', 'RUNE', 'BCH', 'AVAX', 'NEAR', 'ATOM',
              'FIL', '1INCH', 'DOT', 'AAVE', 'ZRX', 'EOS', 'ENJ', 'ADA', 'UNI', 'ALGO', 'SNX', 'LTC', 'XTZ', 'COMP',
              'MKR', 'XLM', 'YFI', 'SUSHI', 'XMR', 'ETC', 'ZEC', 'CELO', 'TRX', 'ICP', 'UMA']

    def create_update_dict(self):
        return {
            token: {
                'token': token,
                'bidPrice': -1,
                'askPrice': -1,
                'time': -1,
                'volume24h': -1
            }
            for token in self.tokens
        }
