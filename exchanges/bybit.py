from . import Exchange


class BybitFuturesExchange(Exchange):
    table_name = 'BYBIT_fut_data'
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
