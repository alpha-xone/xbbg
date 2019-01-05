import pandas as pd

import sys
import pytest

from xbbg import const
from xbbg.io import files, logs, storage
from xbbg.core import utils, assist

from xbbg.core.timezone import DEFAULT_TZ
from xbbg.core.conn import with_bloomberg, create_connection

if hasattr(pytest, 'config'):
    if not pytest.config.option.get('with_bbg', False):
        pytest.skip('no Bloomberg')

if hasattr(sys, 'pytest_call'): create_connection()


@with_bloomberg
def bdp(tickers, flds, cache=False, **kwargs):
    """
    Get reference data and save to

    Args:
        tickers: tickers
        flds: fields to query
        cache: bool - use cache to store data
        **kwargs: overrides

    Returns:
        pd.DataFrame

    Examples:
        >>> bdp('IQ US Equity', 'Crncy', raw=True)
                 ticker  field value
        0  IQ US Equity  Crncy   USD
        >>> bdp('IQ US Equity', 'Crncy').reset_index()
                 ticker crncy
        0  IQ US Equity   USD
    """
    logger = logs.get_logger(bdp, level=kwargs.pop('log', logs.LOG_LEVEL))
    con, _ = create_connection()
    ovrds = assist.proc_ovrds(**kwargs)

    logger.info(
        f'loading reference data from Bloomberg:\n'
        f'{assist.info_qry(tickers=tickers, flds=flds)}'
    )
    data = con.ref(tickers=tickers, flds=flds, ovrds=ovrds)
    if not cache: return [data]

    qry_data = []
    for r, snap in data.iterrows():
        subset = [r]
        data_file = storage.ref_file(
            ticker=snap.ticker, fld=snap.field, ext='pkl', cache=cache, **kwargs
        )
        if data_file:
            if not files.exists(data_file): qry_data.append(data.iloc[subset])
            files.create_folder(data_file, is_file=True)
            data.iloc[subset].to_pickle(data_file)

    return qry_data


@with_bloomberg
def bds(tickers, flds, cache=False, **kwargs):
    """
    Download block data from Bloomberg

    Args:
        tickers: ticker(s)
        flds: field(s)
        cache: whether read from cache
        **kwargs: other overrides for query
          -> raw: raw output from `pdbdp` library, default False

    Returns:
        pd.DataFrame: block data

    Examples:
        >>> import os
        >>>
        >>> pd.options.display.width = 120
        >>> s_dt, e_dt = '20180301', '20181031'
        >>> dvd = bds(
        ...     'NVDA US Equity', 'DVD_Hist_All',
        ...     DVD_Start_Dt=s_dt, DVD_End_Dt=e_dt, raw=True,
        ... )
        >>> dvd.loc[:, ['ticker', 'name', 'value']].head(8)
                   ticker                name         value
        0  NVDA US Equity       Declared Date    2018-08-16
        1  NVDA US Equity             Ex-Date    2018-08-29
        2  NVDA US Equity         Record Date    2018-08-30
        3  NVDA US Equity        Payable Date    2018-09-21
        4  NVDA US Equity     Dividend Amount          0.15
        5  NVDA US Equity  Dividend Frequency       Quarter
        6  NVDA US Equity       Dividend Type  Regular Cash
        7  NVDA US Equity       Declared Date    2018-05-10
        >>> dvd = bds(
        ...     'NVDA US Equity', 'DVD_Hist_All',
        ...     DVD_Start_Dt=s_dt, DVD_End_Dt=e_dt,
        ... )
        >>> dvd.reset_index().loc[:, ['ticker', 'ex_date', 'dividend_amount']]
                   ticker     ex_date  dividend_amount
        0  NVDA US Equity  2018-08-29             0.15
        1  NVDA US Equity  2018-05-23             0.15
        >>> if not os.environ.get('BBG_ROOT', ''):
        ...     os.environ['BBG_ROOT'] = f'{files.abspath(__file__, 1)}/tests/data'
        >>> idx_kw = dict(End_Dt='20181220', cache=True)
        >>> idx_wt = bds('DJI Index', 'Indx_MWeight_Hist', **idx_kw)
        >>> idx_wt.round(2).tail().reset_index(drop=True)
          index_member  percent_weight
        0         V UN            3.82
        1        VZ UN            1.63
        2       WBA UW            2.06
        3       WMT UN            2.59
        4       XOM UN            2.04
        >>> idx_wt = bds('DJI Index', 'Indx_MWeight_Hist', **idx_kw)
        >>> idx_wt.round(2).head().reset_index(drop=True)
          index_member  percent_weight
        0      AAPL UW            4.65
        1       AXP UN            2.84
        2        BA UN            9.29
        3       CAT UN            3.61
        4      CSCO UW            1.26
    """
    logger = logs.get_logger(bds, level=kwargs.pop('log', logs.LOG_LEVEL))
    has_date = kwargs.pop('has_date', True)
    con, _ = create_connection()
    ovrds = assist.proc_ovrds(**kwargs)

    logger.info(
        f'loading block data from Bloomberg:\n'
        f'{assist.info_qry(tickers=tickers, flds=flds)}'
    )
    data = con.bulkref(tickers=tickers, flds=flds, ovrds=ovrds)
    if not cache: return [data]

    qry_data = []
    for (ticker, fld), grp in data.groupby(['ticker', 'field']):
        data_file = storage.ref_file(
            ticker=ticker, fld=fld, has_date=has_date, ext='pkl', cache=cache, **kwargs
        )
        if data_file:
            if not files.exists(data_file): qry_data.append(grp)
            files.create_folder(data_file, is_file=True)
            grp.reset_index(drop=True).to_pickle(data_file)

    return qry_data


@with_bloomberg
def bdh(tickers, flds, start_date, end_date='today', adjust=None, **kwargs):
    """
    Bloomberg historical data

    Args:
        tickers: ticker(s)
        flds: field(s)
        start_date: start date
        end_date: end date - default today
        adjust: `all`, `dvd`, `normal`, `abn` (=abnormal), `split`, `-` or None
                exact match of above words will adjust for corresponding events
                Case 0: `-` no adjustment for dividend or split
                Case 1: `dvd` or `normal|abn` will adjust for all dividends except splits
                Case 2: `adjust` will adjust for splits and ignore all dividends
                Case 3: `all` == `dvd|split` == adjust for all
                Case 4: None == Bloomberg default OR use kwargs
        **kwargs: overrides

    Returns:
        pd.DataFrame

    Examples:
        >>> bdh(
        ...     tickers='VIX Index', flds=['High', 'Low', 'Last_Price'],
        ...     start_date='2018-02-05', end_date='2018-02-07',
        ... ).round(2).transpose().rename_axis((None, None))
                              2018-02-05  2018-02-06  2018-02-07
        VIX Index High             38.80       50.30       31.64
                  Low              16.80       22.42       21.17
                  Last_Price       37.32       29.98       27.73
        >>> bdh(
        ...     tickers='AAPL US Equity', flds='Px_Last',
        ...     start_date='20140605', end_date='20140610', adjust='-'
        ... ).round(2)
        ticker     AAPL US Equity
        field             Px_Last
        2014-06-05         647.35
        2014-06-06         645.57
        2014-06-09          93.70
        2014-06-10          94.25
        >>> bdh(
        ...     tickers='AAPL US Equity', flds='Px_Last',
        ...     start_date='20140606', end_date='20140609',
        ...     CshAdjNormal=False, CshAdjAbnormal=False, CapChg=False,
        ... ).round(2)
        ticker     AAPL US Equity
        field             Px_Last
        2014-06-06         645.57
        2014-06-09          93.70
    """
    logger = logs.get_logger(bdh, level=kwargs.pop('log', logs.LOG_LEVEL))

    # Dividend adjustments
    if isinstance(adjust, str) and adjust:
        if adjust == 'all':
            kwargs['CshAdjNormal'] = True
            kwargs['CshAdjAbnormal'] = True
            kwargs['CapChg'] = True
        else:
            kwargs['CshAdjNormal'] = 'normal' in adjust or 'dvd' in adjust
            kwargs['CshAdjAbnormal'] = 'abn' in adjust or 'dvd' in adjust
            kwargs['CapChg'] = 'split' in adjust

    con, _ = create_connection()
    elms = assist.proc_elms(**kwargs)
    ovrds = assist.proc_ovrds(**kwargs)

    if isinstance(tickers, str): tickers = [tickers]
    if isinstance(flds, str): flds = [flds]
    s_dt = utils.fmt_dt(start_date, fmt='%Y%m%d')
    e_dt = utils.fmt_dt(end_date, fmt='%Y%m%d')

    logger.info(
        f'loading historical data from Bloomberg:\n'
        f'{assist.info_qry(tickers=tickers, flds=flds)}'
    )

    return con.bdh(
        tickers=tickers, flds=flds, elms=elms, ovrds=ovrds,
        start_date=s_dt, end_date=e_dt,
    ).rename_axis(None)


@with_bloomberg
def bdib(ticker, dt, typ='TRADE', batch=False, log=logs.LOG_LEVEL) -> pd.DataFrame:
    """
    Download intraday data and save to cache

    Args:
        ticker: ticker name
        dt: date to download
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
        batch: whether is batch process to download data
        log: level of logs

    Returns:
        pd.DataFrame
    """
    from xbbg.core import missing

    logger = logs.get_logger(bdib, level=log)

    t_1 = pd.Timestamp('today').date() - pd.Timedelta('1D')
    whole_day = pd.Timestamp(dt).date() < t_1
    if (not whole_day) and batch:
        logger.warning(f'querying date {t_1} is too close, ignoring download ...')
        return pd.DataFrame()

    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    asset = ticker.split()[-1]
    info_log = f'{ticker} / {cur_dt} / {typ}'

    if asset in ['Equity', 'Curncy', 'Index', 'Comdty']:
        exch = const.exch_info(ticker=ticker)
        if exch.empty: return pd.DataFrame()
    else:
        logger.error(f'unknown asset type: {asset}')
        return pd.DataFrame()

    time_fmt = '%Y-%m-%dT%H:%M:%S'
    time_idx = pd.DatetimeIndex([
        f'{cur_dt} {exch.allday[0]}', f'{cur_dt} {exch.allday[-1]}']
    ).tz_localize(exch.tz).tz_convert(DEFAULT_TZ).tz_convert('UTC')
    if time_idx[0] > time_idx[1]: time_idx -= pd.TimedeltaIndex(['1D', '0D'])

    q_tckr = ticker
    if exch.get('is_fut', False):
        if 'freq' not in exch:
            logger.error(f'[freq] missing in info for {info_log} ...')

        is_sprd = exch.get('has_sprd', False) and (len(ticker[:-1]) != exch['tickers'][0])
        if not is_sprd:
            q_tckr = fut_ticker(gen_ticker=ticker, dt=dt, freq=exch['freq'])
            if q_tckr == '':
                logger.error(f'cannot find futures ticker for {ticker} ...')
                return pd.DataFrame()

    info_log = f'{q_tckr} / {cur_dt} / {typ}'
    miss_kw = dict(ticker=ticker, dt=dt, typ=typ, func='bdib')
    cur_miss = missing.current_missing(**miss_kw)
    if cur_miss >= 2:
        if batch: return pd.DataFrame()
        logger.info(f'{cur_miss} trials with no data {info_log}')
        return pd.DataFrame()

    logger.info(f'loading data from Bloomberg: {info_log} ...')
    con, _ = create_connection()
    data = con.bdib(
        ticker=q_tckr, event_type=typ, interval=1,
        start_datetime=time_idx[0].strftime(time_fmt),
        end_datetime=time_idx[1].strftime(time_fmt),
    )

    if not isinstance(data, pd.DataFrame):
        raise ValueError(f'unknown output format: {type(data)}')

    if data.empty:
        logger.warning(f'no data for {info_log} ...')
        missing.update_missing(**miss_kw)
        return pd.DataFrame()

    data = data.tz_localize('UTC').tz_convert(exch.tz)
    storage.save_intraday(data=data, ticker=ticker, dt=dt, typ=typ)

    return pd.DataFrame() if batch else assist.format_intraday(data=data, ticker=ticker)


def intraday(ticker, dt, session='', start_time=None, end_time=None, typ='TRADE'):
    """
    Retrieve interval data for ticker

    Args:
        ticker: ticker
        dt: date
        session: examples include
                 day_open_30, am_normal_30_30, day_close_30, allday_exact_0930_1000
        start_time: start time
        end_time: end time
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]

    Returns:
        pd.DataFrame
    """
    from xbbg.core import intervals

    cur_data = bdib(ticker=ticker, dt=dt, typ=typ)
    if cur_data.empty: return pd.DataFrame()

    fmt = '%H:%M:%S'
    ss = intervals.SessNA
    if session: ss = intervals.get_interval(ticker=ticker, session=session)

    if ss != intervals.SessNA:
        start_time = pd.Timestamp(ss.start_time).strftime(fmt)
        end_time = pd.Timestamp(ss.end_time).strftime(fmt)

    if start_time and end_time:
        return cur_data.between_time(start_time=start_time, end_time=end_time)

    return cur_data


@with_bloomberg
def earning(ticker, by='Geo', cache=False, **kwargs):
    """
    Earning exposures by Geo or Products

    Args:
        ticker: ticker name
        by: [G(eo), P(roduct)]
        cache: whether to load from cache

    Returns:
        pd.DataFrame

    Examples:
        >>> data = earning('AMD US Equity', Eqy_Fund_Year=2017, Number_Of_Periods=1)
        >>> data.round(2)
                         level  fy2017  fy2017_pct
        Asia-Pacific       1.0  3540.0       66.43
           China           2.0  1747.0       49.35
           Japan           2.0  1242.0       35.08
           Singapore       2.0   551.0       15.56
        United States      1.0  1364.0       25.60
        Europe             1.0   263.0        4.94
        Other Countries    1.0   162.0        3.04
    """
    ovrd = 'G' if by[0].upper() == 'G' else 'P'
    new_kw = dict(cache=cache, raw=True, Product_Geo_Override=ovrd)
    header = bds(tickers=ticker, flds='PG_Bulk_Header', **new_kw, **kwargs)
    data = bds(tickers=ticker, flds='PG_Revenue', **new_kw, **kwargs)
    return assist.format_earning(data=data, header=header)


def dividend(tickers, typ='all', start_date=None, end_date=None, **kwargs):
    """
    Dividend history

    Args:
        tickers: list of tickers
        typ: `all`, `dvd`, `split`, `split`, `gross`, `adjust`, `adj_fund`,
             `with_amt`, `dvd_amt`, `gross_amt`, `projected`
        start_date: start date
        end_date: end date
        **kwargs: overrides

    Returns:
        pd.DataFrame

    Examples:
        >>> dividend(
        ...     tickers=['C US Equity', 'NVDA US Equity', 'MS US Equity'],
        ...     start_date='2018-01-01', end_date='2018-05-01'
        ... ).rename_axis(None).loc[:, [
        ...     'ex_date', 'rec_date', 'dvd_amt'
        ... ]].round(2)
                           ex_date    rec_date  dvd_amt
        C US Equity     2018-02-02  2018-02-05     0.32
        MS US Equity    2018-04-27  2018-04-30     0.25
        MS US Equity    2018-01-30  2018-01-31     0.25
        NVDA US Equity  2018-02-22  2018-02-23     0.15
    """
    if isinstance(tickers, str): tickers = [tickers]
    tickers = [t for t in tickers if ('Equity' in t) and ('=' not in t)]

    fld = {
        'all': 'DVD_Hist_All', 'dvd': 'DVD_Hist',
        'split': 'Eqy_DVD_Hist_Splits', 'gross': 'Eqy_DVD_Hist_Gross',
        'adjust': 'Eqy_DVD_Adjust_Fact', 'adj_fund': 'Eqy_DVD_Adj_Fund',
        'with_amt': 'DVD_Hist_All_with_Amt_Status',
        'dvd_amt': 'DVD_Hist_with_Amt_Status',
        'gross_amt': 'DVD_Hist_Gross_with_Amt_Stat',
        'projected': 'BDVD_Pr_Ex_Dts_DVD_Amts_w_Ann',
    }.get(typ, typ)

    if (fld == 'Eqy_DVD_Adjust_Fact') and ('Corporate_Actions_Filter' not in kwargs):
        kwargs['Corporate_Actions_Filter'] = 'NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE'

    if fld in [
        'DVD_Hist_All', 'DVD_Hist', 'Eqy_DVD_Hist_Gross',
        'DVD_Hist_All_with_Amt_Status', 'DVD_Hist_with_Amt_Status',
    ]:
        if start_date: kwargs['DVD_Start_Dt'] = utils.fmt_dt(start_date, fmt='%Y%m%d')
        if end_date: kwargs['DVD_End_Dt'] = utils.fmt_dt(end_date, fmt='%Y%m%d')

    kwargs['col_maps'] = {
        'Declared Date': 'dec_date', 'Ex-Date': 'ex_date',
        'Record Date': 'rec_date', 'Payable Date': 'pay_date',
        'Dividend Amount': 'dvd_amt', 'Dividend Frequency': 'dvd_freq',
        'Dividend Type': 'dvd_type', 'Amount Status': 'amt_status',
        'Adjustment Date': 'adj_date', 'Adjustment Factor': 'adj_factor',
        'Adjustment Factor Operator Type': 'adj_op',
        'Adjustment Factor Flag': 'adj_flag',
        'Amount Per Share': 'amt_ps', 'Projected/Confirmed': 'category',
    }

    return bds(tickers=tickers, flds=fld, raw=False, **kwargs)


@with_bloomberg
def active_futures(ticker: str, dt):
    """
    Active futures contract

    Args:
        ticker: futures ticker, i.e., ESA Index, Z A Index, CLA Comdty, etc.
        dt: date

    Returns:
        str: ticker name
    """
    t_info = ticker.split()
    prefix, asset = ' '.join(t_info[:-1]), t_info[-1]
    info = const.market_info(f'{prefix[:-1]}1 {asset}')

    f1, f2 = f'{prefix[:-1]}1 {asset}', f'{prefix[:-1]}2 {asset}'
    fut_2 = fut_ticker(gen_ticker=f2, dt=dt, freq=info['freq'])
    fut_1 = fut_ticker(gen_ticker=f1, dt=dt, freq=info['freq'])

    fut_tk = bdp(tickers=[fut_1, fut_2], flds='Last_Tradeable_Dt', cache=True)

    if pd.Timestamp(dt).month < pd.Timestamp(fut_tk.value[0]).month: return fut_1

    d1 = bdib(ticker=f1, dt=dt)
    d2 = bdib(ticker=f2, dt=dt)

    return fut_1 if d1.volume.sum() > d2.volume.sum() else fut_2


@with_bloomberg
def fut_ticker(gen_ticker: str, dt, freq: str, log=logs.LOG_LEVEL):
    """
    Get proper ticker from generic ticker

    Args:
        gen_ticker: generic ticker
        dt: date
        freq: futures contract frequency
        log: level of logs

    Returns:
        str: exact futures ticker
    """
    logger = logs.get_logger(fut_ticker, level=log)
    dt = pd.Timestamp(dt)
    t_info = gen_ticker.split()

    asset = t_info[-1]
    if asset in ['Index', 'Curncy', 'Comdty']:
        ticker = ' '.join(t_info[:-1])
        prefix, idx, postfix = ticker[:-1], int(ticker[-1]) - 1, asset

    elif asset == 'Equity':
        ticker = t_info[0]
        prefix, idx, postfix = ticker[:-1], int(ticker[-1]) - 1, ' '.join(t_info[1:])

    else:
        logger.error(f'unkonwn asset type for ticker: {gen_ticker}')
        return ''

    month_ext = 4 if asset == 'Comdty' else 2
    months = pd.DatetimeIndex(start=dt, periods=max(idx + month_ext, 3), freq=freq)
    logger.debug(f'pulling expiry dates for months: {months}')

    def to_fut(month):
        return prefix + const.Futures[month.strftime('%b')] + \
               month.strftime('%y')[-1] + ' ' + postfix

    fut = [to_fut(m) for m in months]
    logger.debug(f'trying futures: {fut}')
    # noinspection PyBroadException
    try:
        fut_matu = bdp(tickers=fut, flds='last_tradeable_dt', cache=True)
    except Exception as e1:
        logger.error(f'error downloading futures contracts (1st trial) {e1}:\n{fut}')
        # noinspection PyBroadException
        try:
            fut = fut[:-1]
            logger.debug(f'trying futures (2nd trial): {fut}')
            fut_matu = bdp(tickers=fut, flds='last_tradeable_dt', cache=True)
        except Exception as e2:
            logger.error(f'error downloading futures contracts (2nd trial) {e2}:\n{fut}')
            return ''

    sub_fut = fut_matu[pd.DatetimeIndex(fut_matu.value) > dt]
    logger.debug(f'futures full chain:\n{fut_matu.to_string()}')
    logger.debug(f'getting index {idx} from:\n{sub_fut.to_string()}')
    return sub_fut.ticker.values[idx]


@with_bloomberg
def check_hours(tickers, tz_exch, tz_loc=DEFAULT_TZ):
    """
    Check exchange hours for tickers

    Args:
        tickers: list of tickers
        tz_exch: exchange timezone
        tz_loc: local timezone

    Returns:
        Local and exchange hours
    """
    cols = ['Trading_Day_Start_Time_EOD', 'Trading_Day_End_Time_EOD']
    con, _ = create_connection()
    hours = con.ref(tickers=tickers, flds=cols)
    cur_dt = pd.Timestamp('today').strftime('%Y-%m-%d ')
    hours.loc[:, 'local'] = hours.value.astype(str).str[:-3]
    hours.loc[:, 'exch'] = pd.DatetimeIndex(
        cur_dt + hours.value.astype(str)
    ).tz_localize(tz_loc).tz_convert(tz_exch).strftime('%H:%M')

    hours = pd.concat([
        hours.set_index(['ticker', 'field']).exch.unstack().loc[:, cols],
        hours.set_index(['ticker', 'field']).local.unstack().loc[:, cols],
    ], axis=1)
    hours.columns = ['Exch_Start', 'Exch_End', 'Local_Start', 'Local_End']

    return hours
