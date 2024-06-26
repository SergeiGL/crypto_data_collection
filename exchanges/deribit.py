from . import Exchange


class DeribitFuturesExchange(Exchange):
    table_name = 'DERIBIT_fut_data'
    _template = '{coin}_USDC-PERPETUAL'
    _coin_re = r'^(\w+)_USDC-PERPETUAL$'
    _coins = ['BTC', 'ETH', 'SOL', 'XRP', 'LINK', 'DOGE', 'MATIC', 'BCH', 'AVAX', 'NEAR', 'DOT', 'ADA', 'UNI', 'ALGO',
              'LTC']
