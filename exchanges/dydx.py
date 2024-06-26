from . import Exchange


class DydxFuturesExchange(Exchange):
    table_name = 'DYDX_data'
    _template = '{coin}-USD'
    _coin_re = r'^(\w+)-USD$'
    _coins = ['CELO', 'LINK', 'DOGE', '1INCH', 'XMR', 'FIL', 'ETH', 'AAVE', 'ATOM', 'MKR', 'EOS', 'COMP', 'ALGO', 'XTZ',
              'UNI', 'ADA', 'ZRX', 'YFI', 'MATIC', 'ETC', 'AVAX', 'LTC', 'ENJ', 'DOT', 'SNX', 'RUNE', 'XLM', 'BCH',
              'TRX', 'BTC', 'UMA', 'NEAR', 'ZEC', 'SOL', 'SUSHI', 'ICP', 'CRV']
