from xbbg.exchange import ExchHours

Comdty = [
    dict(
        tickers=['CL'],
        exch=ExchHours.NYME, freq='M', is_fut=True,
    ),
    dict(
        tickers=['CO', 'XW'],
        exch=ExchHours.FuturesEuropeICE, freq='M', is_fut=True,
    ),
    dict(
        tickers=['IOE'],
        exch=ExchHours.CommoditiesDalian,
        freq='M', is_fut=True, key_month=['J', 'K', 'U'],
    ),
    dict(
        tickers=['RBT'],
        exch=ExchHours.CommoditiesDalian,
        freq='M', is_fut=True, key_month=['J', 'K', 'U'],
    ),
    dict(
        tickers=['HG'],
        exch=ExchHours.CMX, freq='Q', is_fut=True,
    ),
]
