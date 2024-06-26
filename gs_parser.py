from sheet import get_sheet, SheetValues

from models.notify import NotifyType, NotifyRule


def filter_tokens(values: list[str]) -> set[str]:
    return {s.strip().upper() for s in values if s}


def filter_exchanges(values: list[str]) -> set[str]:
    return {s.strip().lower() for s in values if s}


def s2float(s: str) -> float:
    if s is None:
        return s
    return float(s.replace(',', '.'))


def demo_rules():
    main_tokens = {'BTC', 'ETH'}
    other_tokens = {
        '1INCH', 'AAVE', 'ACE', 'ADA', 'AGIX', 'AGLD', 'ALGO', 'ALPHA', 'ANT', 'APE', 'API3', 'APT', 'AR', 'ARB',
        'ATOM', 'AUCTION', 'AVAX', 'AXS', 'BADGER', 'BAL', 'BAND', 'BAT', 'BCH', 'BICO', 'BIGTIME', 'BLUR', 'BNB',
        'BNT', 'BSV', 'BTC', 'CELO', 'CFX', 'CHZ', 'COMP', 'CRV', 'DOGE', 'DOT', 'DYDX', 'EGLD', 'ENJ', 'ENS',
        'EOS', 'ETC', 'ETH', 'ETHW', 'FET', 'FIL', 'FLM', 'FLOW', 'FRONT', 'FTM', 'GALA', 'GAS', 'GMT', 'GMX',
        'GRT', 'HBAR', 'ICP', 'ID', 'IMX', 'INJ', 'IOST', 'IOTA', 'KLAY', 'KNC', 'KSM', 'LDO', 'LINK', 'LPT', 'LRC',
        'LTC', 'MAGIC', 'MANA', 'MASK', 'MATIC', 'MEME', 'MINA', 'MKR', 'NEAR', 'NEO', 'OMG', 'ONT', 'OP', 'ORBS',
        'ORDI', 'PEOPLE', 'PERP', 'PYTH', 'QTUM', 'RDNT', 'REN', 'RNDR', 'RSR', 'RUNE', 'RVN', 'SAND', 'SLP', 'SNX',
        'SOL', 'SSV', 'STORJ', 'STX', 'SUI', 'SUSHI', 'THETA', 'TIA', 'TRB', 'TRX', 'UMA', 'UNI', 'USDC', 'USTC',
        'WAVES', 'WAXP', 'WLD', 'WOO', 'XLM', 'XMR', 'XRP', 'XTZ', 'YFI', 'YGG', 'ZEC', 'ZIL', 'ZRX',
    }
    rules = [
        NotifyRule(typ=NotifyType.price_alerts_only, tokens=other_tokens - main_tokens,
                   exchanges={'dydx', 'okx', 'binance'}, sx=-1),
        NotifyRule(typ=NotifyType.price_alerts_only, tokens=main_tokens,
                   exchanges={'dydx', 'okx', 'binance', 'bybit'}, sx=-0.5),

        NotifyRule(typ=NotifyType.funding_rates_alerts_only, tokens=other_tokens - main_tokens,
                   exchanges={'dydx', 'okx', 'binance'}, fx=300.0),
        NotifyRule(typ=NotifyType.funding_rates_alerts_only, tokens=main_tokens,
                   exchanges={'dydx', 'okx', 'binance', 'bybit'}, fx=100.0),

        NotifyRule(typ=NotifyType.funding_margin_rates_alerts, tokens=other_tokens | main_tokens,
                   exchanges={'binance'}, mf1=30, mf2=10),

        NotifyRule(typ=NotifyType.price_and_funding_rates_alerts, tokens=other_tokens - main_tokens,
                   exchanges={'dydx', 'okx', 'binance'}, sx=-0.1, fx=100),
        NotifyRule(typ=NotifyType.price_and_funding_rates_alerts, tokens=main_tokens,
                   exchanges={'dydx', 'okx', 'binance', 'bybit'}, sx=-0.1, fx=50),
    ]
    return rules


def get_rules():
    rules = []
    doc = get_sheet()

    data = SheetValues(doc.worksheet('Price alerts only'))
    sx1, sx2 = s2float(data['B4']), s2float(data['F4'])
    other_tokens = filter_tokens(data['A8:A1000'])
    main_tokens = filter_tokens(data['E8:E1000'])
    main_exchanges = filter_exchanges(data['C8:C28'])
    all_exchanges = filter_exchanges(data['G8:G28'])

    rules += [
        NotifyRule(typ=NotifyType.price_alerts_only, tokens=other_tokens - main_tokens,
                   exchanges=main_exchanges, sx=sx1),
        NotifyRule(typ=NotifyType.price_alerts_only, tokens=main_tokens,
                   exchanges=all_exchanges, sx=sx2),
    ]

    data = SheetValues(doc.worksheet('Funding rate alerts only'))
    fx1, fx2 = s2float(data['B4']), s2float(data['F4'])
    other_tokens = filter_tokens(data['A8:A1000'])
    main_tokens = filter_tokens(data['E8:E1000'])
    main_exchanges = filter_exchanges(data['C8:C28'])
    all_exchanges = filter_exchanges(data['G8:G28'])

    rules += [
        NotifyRule(typ=NotifyType.funding_rates_alerts_only, tokens=other_tokens - main_tokens,
                   exchanges=main_exchanges, fx=fx1),
        NotifyRule(typ=NotifyType.funding_rates_alerts_only, tokens=main_tokens,
                   exchanges=all_exchanges, fx=fx2),
    ]

    mf1, mf2 = s2float(data['J4']), s2float(data['J5'])
    all_tokens = filter_tokens(data['I8:I1000'])
    all_exchanges = filter_exchanges(data['K8:K1000'])

    rules += [
        NotifyRule(typ=NotifyType.funding_margin_rates_alerts, tokens=all_tokens,
                   exchanges=all_exchanges, mf1=mf1, mf2=mf2),
    ]

    data = SheetValues(doc.worksheet('Price + Funding rate alerts'))
    sp1, sp2 = s2float(data['B4']), s2float(data['F4'])
    fp1, fp2 = s2float(data['B5']), s2float(data['F5'])
    other_tokens = filter_tokens(data['A8:A1000'])
    main_tokens = filter_tokens(data['E8:E1000'])
    main_exchanges = filter_exchanges(data['C8:C28'])
    all_exchanges = filter_exchanges(data['G8:G28'])

    rules += [
        NotifyRule(typ=NotifyType.price_and_funding_rates_alerts, tokens=other_tokens - main_tokens,
                   exchanges=main_exchanges, sx=sp1, fx=fp1),
        NotifyRule(typ=NotifyType.price_and_funding_rates_alerts, tokens=main_tokens,
                   exchanges=all_exchanges, sx=sp2, fx=fp2),
    ]

    return rules
