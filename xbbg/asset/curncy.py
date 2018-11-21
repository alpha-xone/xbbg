from xbbg.exchange import ExchHours

Curncy = [
    dict(
        tickers=[
            'JPY', 'AUD', 'HKD', 'CNHHKD', 'CNH', 'EUR',
            'GBP', 'GBPAUD', 'GBPEUR', 'AUDGBP', 'GBPHKD',
            'KWN', 'IRN', 'NTN', 'CNH1M', 'ZAR',
        ],
        exch=ExchHours.CurrencyGeneric,
    ),
    dict(
        tickers=['KRW'],
        exch=ExchHours.CurrencySouthKorea,
    ),
    dict(
        tickers=['TWD'],
        exch=ExchHours.CurrencyTaiwan,
    ),
    dict(
        tickers=['CNYHKD', 'CNY'],
        exch=ExchHours.CurrencyChina,
    ),
    dict(
        tickers=['INR'],
        exch=ExchHours.CurrencyIndia,
    ),
    dict(
        tickers=['IRD'],
        exch=ExchHours.CurrencyDubai, freq='M', is_fut=True,
    ),
    dict(
        tickers=['XID'],
        exch=ExchHours.CurrencySingapore, freq='M', is_fut=True,
    ),
    dict(
        tickers=['INT'],
        exch=ExchHours.CurrencyIndia, freq='M', is_fut=True,
    ),
    dict(
        tickers=['DXY'],
        exch=ExchHours.CurrencyICE,
    ),
]
