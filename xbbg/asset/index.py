from xbbg.exchange import ExchHours

Index = [
    dict(
        tickers=['XP'],
        exch=ExchHours.FuturesAustralia, freq='Q', is_fut=True,
    ),
    dict(
        tickers=['AS51'],
        exch=ExchHours.IndexAustralia,
    ),
    dict(
        tickers=['NKY', 'TPX'],
        exch=ExchHours.EquityJapan,
    ),
    dict(
        tickers=['NK', 'TP'],
        exch=ExchHours.FuturesJapan, freq='Q', is_fut=True,
    ),
    dict(
        tickers=['KOSPI2', 'KOSPBMET'],
        exch=ExchHours.IndexSouthKorea,
    ),
    dict(
        tickers=['KM'],
        exch=ExchHours.FuturesSouthKorea, freq='Q', is_fut=True,
    ),
    dict(
        tickers=['TWSE'],
        exch=ExchHours.EquityTaiwan,
    ),
    dict(
        tickers=['TW'],
        exch=ExchHours.FuturesTaiwan, freq='M', is_fut=True,
    ),
    dict(
        tickers=['HSI', 'HSCEI'],
        exch=ExchHours.EquityHongKong,
    ),
    dict(
        tickers=['HI', 'HC'],
        exch=ExchHours.FuturesHongKong, freq='M', is_fut=True,
    ),
    dict(
        tickers=['SHSZ300', 'SHCOMP', 'SZCOMP', 'SZ399006', 'SH000905'],
        exch=ExchHours.EquityChina,
    ),
    dict(
        tickers=[
            'SPX', 'INDU', 'CCMP', 'RTY', 'RAY', 'SOX',
            'S5FINL', 'S5INSU', 'S5TRAN', 'S5UTIL', 'S5ENRS', 'VIX',
            'S5IOIL', 'S5STEL', 'S5ITEL', 'S5SECO', 'S5INFT', 'SOX',
            'EWAIV', 'EWJIV', 'EWYIV', 'EWTIV', 'EWHIV', 'FXIIV', 'INDAIV',
            'AIAIV', 'AAXJIV', 'EEMIV',
        ],
        exch=ExchHours.IndexUS,
    ),
    dict(
        tickers=['XU'],
        exch=ExchHours.FuturesSingapore, freq='M', is_fut=True,
    ),
    dict(
        tickers=['TTMT', 'HDFCB', 'WPRO', 'INFO', 'ICICI', 'DRRD', 'VEDL'],
        exch=ExchHours.EquityFuturesIndia, freq='M', is_fut=True,
    ),
    dict(
        tickers=['NZ'],
        exch=ExchHours.IndexFuturesIndia, freq='M', is_fut=True,
    ),
    dict(
        tickers=['NIFTY'],
        exch=ExchHours.IndexFuturesIndia,
    ),
    dict(
        tickers=['ES', 'DM', 'NQ'],
        exch=ExchHours.CME, freq='Q', is_fut=True,
    ),
    dict(
        tickers=['Z'],
        exch=ExchHours.FuturesFinancialsICE, freq='Q', is_fut=True,
    ),
    dict(
        tickers=['UX'],
        exch=ExchHours.FuturesCBOE, freq='M', is_fut=True, has_sprd=True,
    ),
    dict(
        tickers=['UKX', 'SXXE'],
        exch=ExchHours.IndexLondon,
    ),
    dict(
        tickers=['SX5E', 'SX5P'],
        exch=ExchHours.IndexEurope1,
    ),
    dict(
        tickers=['BE500'],
        exch=ExchHours.IndexEurope2,
    ),
    dict(
        tickers=['MSER', 'MSPE'],
        exch=ExchHours.IndexEurope3,
    ),
    dict(
        tickers=['USGG2YR', 'USGG10YR', 'USYC2Y10', 'USYC1030', 'USGG30YR'],
        exch=ExchHours.IndexUS,
    ),
    dict(
        tickers=['MES'],
        exch=ExchHours.FuturesNYFICE, freq='Q', is_fut=True,
    ),
]
