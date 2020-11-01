import pandas as pd

from collections import namedtuple

from xbbg.io import logs, param
from xbbg.core import timezone

Futures = dict(
    Jan='F', Feb='G', Mar='H', Apr='J', May='K', Jun='M',
    Jul='N', Aug='Q', Sep='U', Oct='V', Nov='X', Dec='Z',
)
CurrencyPair = namedtuple('CurrencyPair', ['ticker', 'factor', 'power'])
ValidSessions = ['allday', 'day', 'am', 'pm', 'night', 'pre', 'post']


def exch_info(ticker: str, **kwargs) -> pd.Series:
    """
    Exchange info for given ticker

    Args:
        ticker: ticker or exchange
        **kwargs:
            ref: reference ticker or exchange
                 used as supplement if exchange info is not defined for `ticker`

    Returns:
        pd.Series

    Examples:
        >>> exch_info('SPY US Equity')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        pre         [04:00, 09:30]
        post        [16:01, 20:00]
        dtype: object
        >>> exch_info('SPY US Equity', ref='EquityUS')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        pre         [04:00, 09:30]
        post        [16:01, 20:00]
        dtype: object
        >>> exch_info('ES1 Index')
        tz        America/New_York
        allday      [18:00, 17:00]
        day         [08:00, 17:00]
        dtype: object
        >>> exch_info('ESM0 Index', ref='ES1 Index')
        tz        America/New_York
        allday      [18:00, 17:00]
        day         [08:00, 17:00]
        dtype: object
        >>> exch_info('Z 1 Index')
        tz         Europe/London
        allday    [01:00, 21:00]
        day       [01:00, 21:00]
        dtype: object
        >>> exch_info('TESTTICKER Corp').empty
        True
        >>> exch_info('US')
        tz        America/New_York
        allday      [04:00, 20:00]
        day         [09:30, 16:00]
        pre         [04:00, 09:30]
        post        [16:01, 20:00]
        dtype: object
    """
    logger = logs.get_logger(exch_info, level='debug')

    if kwargs.get('ref', None): ticker = kwargs['ref']
    all_exch = param.load_info(cat='exch')
    if ticker in all_exch:
        info = all_exch[ticker]
    else:
        if ' ' not in ticker.strip():
            ticker = f'XYZ {ticker.strip()} Equity'
        info = all_exch.get(
            market_info(ticker=ticker).get('exch', ticker), dict()
        )
    if ('allday' in info) and ('day' not in info):
        info['day'] = info['allday']

    if any(req not in info for req in ['tz', 'allday', 'day']):
        logger.error(f'required exchange info cannot be found in {ticker} ...')
        return pd.Series(dtype=object)

    for ss in ValidSessions:
        if ss not in info: continue
        info[ss] = [param.to_hour(num=s) for s in info[ss]]

    return pd.Series(info)


def market_info(ticker: str) -> dict:
    """
    Get info for given market

    Args:
        ticker: Bloomberg full ticker

    Returns:
        dict

    Examples:
        >>> info_ = market_info('SHCOMP Index')
        >>> info_['exch']
        'EquityChina'
        >>> info_ = market_info('ICICIC=1 IS Equity')
        >>> info_['freq'], info_['is_fut']
        ('M', True)
        >>> info_ = market_info('INT1 Curncy')
        >>> info_['freq'], info_['is_fut']
        ('M', True)
        >>> info_ = market_info('CL1 Comdty')
        >>> info_['freq'], info_['is_fut']
        ('M', True)
        >>> # Wrong tickers
        >>> market_info('C XX Equity')
        {}
        >>> market_info('XXX Comdty')
        {}
        >>> market_info('Bond_ISIN Corp')
        {}
        >>> market_info('XYZ Index')
        {}
        >>> market_info('XYZ Curncy')
        {}
    """
    t_info = ticker.split()
    assets = param.load_info('assets')

    # ========================== #
    #           Equity           #
    # ========================== #

    if (t_info[-1] == 'Equity') and ('=' not in t_info[0]):
        exch = t_info[-2]
        for info in assets.get('Equity', [dict()]):
            if 'exch_codes' not in info: continue
            if exch in info['exch_codes']: return info
        return dict()

    # ============================ #
    #           Currency           #
    # ============================ #

    if t_info[-1] == 'Curncy':
        for info in assets.get('Curncy', [dict()]):
            if 'tickers' not in info: continue
            if (t_info[0].split('+')[0] in info['tickers']) or \
                    (t_info[0][-1].isdigit() and (t_info[0][:-1] in info['tickers'])):
                return info
        return dict()

    if t_info[-1] == 'Comdty':
        for info in assets.get('Comdty', [dict()]):
            if 'tickers' not in info: continue
            end_idx = -2 if t_info[-2].upper() in ['ELEC', 'PIT'] else -1
            if ' '.join(t_info[:-end_idx])[:-1].rstrip() in info['tickers']: return info
        return dict()

    # =================================== #
    #           Index / Futures           #
    # =================================== #

    if (t_info[-1] == 'Index') or (
        (t_info[-1] == 'Equity') and ('=' in t_info[0])
    ):
        if t_info[-1] == 'Equity':
            tck = t_info[0].split('=')[0]
        else:
            tck = ' '.join(t_info[:-1])
        for info in assets.get('Index', [dict()]):
            if 'tickers' not in info: continue
            if (tck[:2] == 'UX') and ('UX' in info['tickers']): return info
            if tck in info['tickers']:
                if t_info[-1] == 'Equity': return info
                if not info.get('is_fut', False): return info
            if tck[:-1].rstrip() in info['tickers']:
                if info.get('is_fut', False): return info
        return dict()

    if t_info[-1] == 'Corp':
        for info in assets.get('Corp', [dict()]):
            if 'ticker' not in info: continue

    return dict()


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
        CurrencyPair(ticker='HKD Curncy', factor=1.0, power=1)
        >>> ccy_pair(local='GBp')
        CurrencyPair(ticker='GBP Curncy', factor=100, power=-1)
        >>> ccy_pair(local='USD', base='GBp')
        CurrencyPair(ticker='GBP Curncy', factor=0.01, power=1)
        >>> ccy_pair(local='XYZ', base='USD')
        CurrencyPair(ticker='', factor=1.0, power=1)
        >>> ccy_pair(local='GBP', base='GBp')
        CurrencyPair(ticker='', factor=0.01, power=1)
        >>> ccy_pair(local='GBp', base='GBP')
        CurrencyPair(ticker='', factor=100.0, power=1)
    """
    ccy_param = param.load_info(cat='ccy')
    if f'{local}{base}' in ccy_param:
        info = ccy_param[f'{local}{base}']

    elif f'{base}{local}' in ccy_param:
        info = ccy_param[f'{base}{local}']
        info['factor'] = 1. / info.get('factor', 1.)
        info['power'] = -info.get('power', 1)

    elif base.lower() == local.lower():
        info = dict(ticker='')
        info['factor'] = 1.
        if base[-1].lower() == base[-1]:
            info['factor'] /= 100.
        if local[-1].lower() == local[-1]:
            info['factor'] *= 100.

    else:
        logger = logs.get_logger(ccy_pair)
        logger.error(f'incorrect currency - local {local} / base {base}')
        return CurrencyPair(ticker='', factor=1., power=1)

    if 'factor' not in info: info['factor'] = 1.
    if 'power' not in info: info['power'] = 1
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
    logger = logs.get_logger(market_timing)
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
