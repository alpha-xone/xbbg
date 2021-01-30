import pandas as pd

from collections import namedtuple
from xbbg.core import timezone
from xbbg.io import files, logs, param

Futures = dict(
    Jan='F', Feb='G', Mar='H', Apr='J', May='K', Jun='M',
    Jul='N', Aug='Q', Sep='U', Oct='V', Nov='X', Dec='Z',
)
CurrencyPair = namedtuple('CurrencyPair', ['ticker', 'factor', 'power'])
ValidSessions = ['allday', 'day', 'am', 'pm', 'night', 'pre', 'post']

PKG_PATH = files.abspath(__file__, 0)

ASSET_INFO = {
    'Index': ['tickers'],
    'Comdty': ['tickers', 'key_month'],
    'Curncy': ['tickers'],
    'Equity': ['exch_codes'],
}

DVD_TPYES = {
    'all': 'DVD_Hist_All',
    'dvd': 'DVD_Hist',
    'split': 'Eqy_DVD_Hist_Splits',
    'gross': 'Eqy_DVD_Hist_Gross',
    'adjust': 'Eqy_DVD_Adjust_Fact',
    'adj_fund': 'Eqy_DVD_Adj_Fund',
    'with_amt': 'DVD_Hist_All_with_Amt_Status',
    'dvd_amt': 'DVD_Hist_with_Amt_Status',
    'gross_amt': 'DVD_Hist_Gross_with_Amt_Stat',
    'projected': 'BDVD_Pr_Ex_Dts_DVD_Amts_w_Ann',
}

DVD_COLS = {
    'Declared Date': 'dec_date',
    'Ex-Date': 'ex_date',
    'Record Date': 'rec_date',
    'Payable Date': 'pay_date',
    'Dividend Amount': 'dvd_amt',
    'Dividend Frequency': 'dvd_freq',
    'Dividend Type': 'dvd_type',
    'Amount Status': 'amt_status',
    'Adjustment Date': 'adj_date',
    'Adjustment Factor': 'adj_factor',
    'Adjustment Factor Operator Type': 'adj_op',
    'Adjustment Factor Flag': 'adj_flag',
    'Amount Per Share': 'amt_ps',
    'Projected/Confirmed': 'category',
}

LIVE_INFO = {
    # Common fields
    'MKTDATA_EVENT_TYPE', 'MKTDATA_EVENT_SUBTYPE', 'IS_DELAYED_STREAM',
    # Last Price
    'LAST_PRICE', 'RT_PX_CHG_PCT_1D', 'REALTIME_PERCENT_BID_ASK_SPREAD',
    'EVT_TRADE_DATE_RT', 'TRADE_UPDATE_STAMP_RT',
    'EQY_TURNOVER_REALTIME', 'VOLUME',
    # Bid
    'BID', 'BID_UPDATE_STAMP_RT',
    # Ask
    'ASK', 'ASK_UPDATE_STAMP_RT',
    # Common in bid / ask
    'SPREAD_BA', 'MID',
}

LIVE_CHG = {
    'RT_PX_CHG_PCT_1D', 'CHG_PCT_1M_RT', 'CHG_PCT_3M_RT',
    'CHG_PCT_MTD_RT', 'CHG_PCT_QTD_RT', 'CHG_PCT_YTD_RT',
    'REALTIME_2_DAY_CHANGE_PERCENT', 'REALTIME_5_DAY_CHANGE_PERCENT',
    'REALTIME_15_SEC_PRICE_PCT_CHG', 'REALTIME_ONE_MIN_PRICE_PCT_CHG',
    # Equities only
    'REALTIME_FIVE_MIN_PRICE_PCT_CHG', 'REALTIME_15_MIN_PRICE_PCT_CHG',
    'REALTIME_ONE_HOUR_PRICE_PCT_CHG',
}

LIVE_VOL = {
    'REALTIME_VOLUME_5_DAY_INTERVAL',
    # Real-time current volume as % change from N-day avg volume
    'DELTA_AVAT_1_DAY_INTERVAL', 'DELTA_AVAT_5_DAY_INTERVAL',
    'DELTA_AVAT_10_DAY_INTERVAL', 'DELTA_AVAT_20_DAY_INTERVAL',
    'DELTA_AVAT_30_DAY_INTERVAL', 'DELTA_AVAT_100_DAY_INTERVAL',
    'DELTA_AVAT_180_DAY_INTERVAL',
    # Real-time turnover as % change from N-day average turnover
    'DELTA_ATAT_1_DAY_INTERVAL', 'DELTA_ATAT_5_DAY_INTERVAL',
    'DELTA_ATAT_10_DAY_INTERVAL', 'DELTA_ATAT_20_DAY_INTERVAL',
    'DELTA_ATAT_30_DAY_INTERVAL', 'DELTA_ATAT_100_DAY_INTERVAL',
    'DELTA_ATAT_180_DAY_INTERVAL',
}

LIVE_RATIO = {
    'PRICE_EARNINGS_RATIO_RT', 'PRICE_TO_BOOK_RATIO_RT',
    'PRICE_TO_SALES_RATIO_RT', 'PRICE_CASH_FLOW_RT', 'PRICE_EBITDA_RT',
}


def exch_info(ticker: str, **kwargs) -> pd.Series:
    """
    Exchange info for given ticker

    Args:
        ticker: ticker or exchange
        **kwargs:
            ref: reference ticker or exchange
                 used as supplement if exchange info is not defined for `ticker`
            original: original ticker (for logging)
            config: info from exch.yml

    Returns:
        pd.Series

    Examples:
        >>> exch_info('SPY US Equity')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        post        [16:01, 20:00]
        pre         [04:00, 09:30]
        Name: EquityUS, dtype: object
        >>> exch_info('SPY US Equity', ref='EquityUS')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        post        [16:01, 20:00]
        pre         [04:00, 09:30]
        Name: EquityUS, dtype: object
        >>> exch_info('ES1 Index')
        tz        America/New_York
        allday      [18:00, 17:00]
        day         [08:00, 17:00]
        Name: CME, dtype: object
        >>> exch_info('ESM0 Index', ref='ES1 Index')
        tz        America/New_York
        allday      [18:00, 17:00]
        day         [08:00, 17:00]
        Name: CME, dtype: object
        >>> exch_info('Z 1 Index')
        tz         Europe/London
        allday    [01:00, 21:00]
        day       [01:00, 21:00]
        Name: FuturesFinancialsICE, dtype: object
        >>> exch_info('TESTTICKER Corp')
        Series([], dtype: object)
        >>> exch_info('US')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        post        [16:01, 20:00]
        pre         [04:00, 09:30]
        Name: EquityUS, dtype: object
        >>> exch_info('UXF1UXG1 Index')
        tz        America/New_York
        allday      [18:00, 17:00]
        day         [18:00, 17:00]
        Name: FuturesCBOE, dtype: object
        >>> exch_info('TESTTICKER Index', original='TESTTICKER Index')
        Series([], dtype: object)
        >>> exch_info('TESTTCK Index')
        Series([], dtype: object)
    """
    logger = logs.get_logger(exch_info, level='debug')

    if kwargs.get('ref', ''):
        return exch_info(ticker=kwargs['ref'])

    exch = kwargs.get('config', param.load_config(cat='exch'))
    original = kwargs.get('original', '')

    # Case 1: Use exchange directly
    if ticker in exch.index:
        info = exch.loc[ticker].dropna()

        # Check required info
        if info.reindex(['allday', 'tz']).dropna().size < 2:
            logger.error(
                f'required info (allday + tz) cannot be found in '
                f'{original if original else ticker} ...'
            )
            return pd.Series(dtype=object)

        # Fill day session info if not provided
        if 'day' not in info:
            info['day'] = info['allday']

        return info.dropna().apply(param.to_hours)

    if original:
        logger.error(f'exchange info cannot be found in {original} ...')
        return pd.Series(dtype=object)

    # Case 2: Use ticker to find exchange
    exch_name = market_info(ticker=ticker).get('exch', '')
    if not exch_name: return pd.Series(dtype=object)
    return exch_info(
        ticker=exch_name,
        original=ticker,
        config=exch,
    )


def market_info(ticker: str) -> pd.Series:
    """
    Get info for given ticker

    Args:
        ticker: Bloomberg full ticker

    Returns:
        dict

    Examples:
        >>> market_info('SHCOMP Index').exch
        'EquityChina'
        >>> market_info('SPY US Equity').exch
        'EquityUS'
        >>> market_info('ICICIC=1 IS Equity').exch
        'EquityFuturesIndia'
        >>> market_info('INT1 Curncy').exch
        'CurrencyIndia'
        >>> market_info('CL1 Comdty').exch
        'NYME'
        >>> incorrect_tickers = [
        ...     'C XX Equity', 'XXX Comdty', 'Bond_ISIN Corp',
        ...     'XYZ Index', 'XYZ Curncy',
        ... ]
        >>> pd.concat([market_info(_) for _ in incorrect_tickers])
        Series([], dtype: object)
    """
    t_info = ticker.split()
    exch_only = len(ticker) == 2
    if (not exch_only) and (t_info[-1] not in ['Equity', 'Comdty', 'Curncy', 'Index']):
        return pd.Series(dtype=object)

    a_info = asset_config(asset='Equity' if exch_only else t_info[-1])

    # =========================================== #
    #           Equity / Equity Futures           #
    # =========================================== #

    if (t_info[-1] == 'Equity') or exch_only:
        is_fut = '==' if '=' in ticker else '!='
        exch_sym = ticker if exch_only else t_info[-2]
        return take_first(
            data=a_info,
            query=f'exch_codes == "{exch_sym}" and is_fut {is_fut} True',
        )

    # ================================================ #
    #           Currency / Commodity / Index           #
    # ================================================ #

    if t_info[0] in a_info.tickers.values:
        symbol = t_info[0]
    elif t_info[0][-1].isdigit():
        end_idx = 2 if t_info[-2].isdigit() else 1
        symbol = t_info[0][:-end_idx].strip()
        # Special contracts
        if (symbol[:2] == 'UX') and (t_info[-1] == 'Index'):
            symbol = 'UX'
    else:
        symbol = t_info[0].split('+')[0]
    return take_first(data=a_info, query=f'tickers == "{symbol}"')


def take_first(data: pd.DataFrame, query: str) -> pd.Series:
    """
    Query and take the 1st row of result

    Args:
        data: pd.DataFrame
        query: query string

    Returns:
        pd.Series
    """
    if data.empty: return pd.Series(dtype=object)
    res = data.query(query)
    if res.empty: return pd.Series(dtype=object)
    return res.reset_index(drop=True).iloc[0]


def asset_config(asset: str) -> pd.DataFrame:
    """
    Load info for given asset

    Args:
        asset: asset name

    Returns:
        pd.DataFrame
    """
    cfg_files = param.config_files('assets')
    cache_cfg = f'{PKG_PATH}/markets/cached/{asset}_cfg.pkl'
    last_mod = max(map(files.modified_time, cfg_files))
    if files.exists(cache_cfg) and files.modified_time(cache_cfg) > last_mod:
        return pd.read_pickle(cache_cfg)

    config = (
        pd.concat([
            explode(
                data=pd.DataFrame(param.load_yaml(cf).get(asset, [])),
                columns=ASSET_INFO[asset],
            )
            for cf in cfg_files
        ], sort=False)
        .drop_duplicates(keep='last')
        .reset_index(drop=True)
    )
    files.create_folder(cache_cfg, is_file=True)
    config.to_pickle(cache_cfg)
    return config


def explode(data: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Explode data by columns

    Args:
        data: pd.DataFrame
        columns: columns to explode

    Returns:
        pd.DataFrame
    """
    if data.empty: return pd.DataFrame()
    if len(columns) == 1:
        return data.explode(column=columns[0])
    return explode(
        data=data.explode(column=columns[-1]),
        columns=columns[:-1],
    )


def ccy_pair(local, base='USD') -> CurrencyPair:
    """
    Currency pair info

    Args:
        local: local currency
        base: base currency

    Returns:
        CurrencyPair

    Examples:
        >>> ccy_pair(local='HKD', base='USD')
        CurrencyPair(ticker='HKD Curncy', factor=1.0, power=1.0)
        >>> ccy_pair(local='GBp')
        CurrencyPair(ticker='GBP Curncy', factor=100.0, power=-1.0)
        >>> ccy_pair(local='USD', base='GBp')
        CurrencyPair(ticker='GBP Curncy', factor=0.01, power=1.0)
        >>> ccy_pair(local='XYZ', base='USD')
        CurrencyPair(ticker='', factor=1.0, power=1.0)
        >>> ccy_pair(local='GBP', base='GBp')
        CurrencyPair(ticker='', factor=0.01, power=1.0)
        >>> ccy_pair(local='GBp', base='GBP')
        CurrencyPair(ticker='', factor=100.0, power=1.0)
    """
    ccy_param = param.load_config(cat='ccy')
    if f'{local}{base}' in ccy_param.index:
        info = ccy_param.loc[f'{local}{base}'].dropna()

    elif f'{base}{local}' in ccy_param.index:
        info = ccy_param.loc[f'{base}{local}'].dropna()
        info['factor'] = 1. / info.get('factor', 1.)
        info['power'] = -info.get('power', 1.)

    elif base.lower() == local.lower():
        info = dict(ticker='')
        info['factor'] = 1.
        if base[-1].lower() == base[-1]:
            info['factor'] /= 100.
        if local[-1].lower() == local[-1]:
            info['factor'] *= 100.

    else:
        logger = logs.get_logger(ccy_pair, level='debug')
        logger.error(f'incorrect currency - local {local} / base {base}')
        return CurrencyPair(ticker='', factor=1., power=1.0)

    if 'factor' not in info: info['factor'] = 1.
    if 'power' not in info: info['power'] = 1.
    return CurrencyPair(**info)


def market_timing(ticker, dt, timing='EOD', tz='local', **kwargs) -> str:
    """
    Market close time for ticker

    Args:
        ticker: ticker name
        dt: date
        timing: [EOD (default), BOD]
        tz: conversion to timezone

    Returns:
        str: date & time

    Examples:
        >>> market_timing('7267 JT Equity', dt='2018-09-10')
        '2018-09-10 14:58'
        >>> market_timing('7267 JT Equity', dt='2018-09-10', tz=timezone.TimeZone.NY)
        '2018-09-10 01:58:00-04:00'
        >>> market_timing('7267 JT Equity', dt='2018-01-10', tz='NY')
        '2018-01-10 00:58:00-05:00'
        >>> market_timing('7267 JT Equity', dt='2018-09-10', tz='SPX Index')
        '2018-09-10 01:58:00-04:00'
        >>> market_timing('8035 JT Equity', dt='2018-09-10', timing='BOD')
        '2018-09-10 09:01'
        >>> market_timing('Z 1 Index', dt='2018-09-10', timing='FINISHED')
        '2018-09-10 21:00'
        >>> market_timing('TESTTICKER Corp', dt='2018-09-10')
        ''
    """
    logger = logs.get_logger(market_timing, level='debug')
    exch = pd.Series(exch_info(ticker=ticker, **kwargs))
    if any(req not in exch.index for req in ['tz', 'allday', 'day']):
        logger.error(f'required exchange info cannot be found in {ticker} ...')
        return ''

    mkt_time = {
        'BOD': exch.day[0], 'FINISHED': exch.allday[-1]
    }.get(timing, exch.day[-1])

    cur_dt = pd.Timestamp(str(dt)).strftime('%Y-%m-%d')
    if tz == 'local': return f'{cur_dt} {mkt_time}'

    return timezone.tz_convert(f'{cur_dt} {mkt_time}', to_tz=tz, from_tz=exch.tz)
