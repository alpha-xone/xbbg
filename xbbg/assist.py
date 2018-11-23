import json
import os
import time
import inflection

import pandas as pd
import pdblp
from xone import utils, files, logs

from xbbg import const
from xbbg.conn import with_bloomberg, create_connection
from xbbg.timezone import DEFAULT_TZ

ELEMENTS = [
    'periodicityAdjustment', 'periodicitySelection', 'currency',
    'nonTradingDayFillOption', 'nonTradingDayFillMethod',
    'maxDataPoints', 'returnEIDs', 'returnRelativeDate',
    'overrideOption', 'pricingOption',
    'adjustmentNormal', 'adjustmentAbnormal', 'adjustmentSplit',
    'adjustmentFollowDPDF', 'calendarCodeOverride',
]

ELEM_KEYS = dict(
    PeriodAdj='periodicityAdjustment', PerAdj='periodicityAdjustment',
    Period='periodicitySelection', Per='periodicitySelection',
    Currency='currency', Curr='currency', FX='currency',
    Days='nonTradingDayFillOption', Fill='nonTradingDayFillMethod', Points='maxDataPoints',
    # 'returnEIDs', 'returnRelativeDate',
    Quote='overrideOption', QuoteType='pricingOption', QtTyp='pricingOption',
    CshAdjNormal='adjustmentNormal', CshAdjAbnormal='adjustmentAbnormal',
    CapChg='adjustmentSplit', UseDPDF='adjustmentFollowDPDF',
    Calendar='calendarCodeOverride',
)

ELEM_VALS = dict(
    periodicityAdjustment=dict(
        A='ACTUAL', C='CALENDAR', F='FISCAL',
    ),
    periodicitySelection=dict(
        D='DAILY', W='WEEKLY', M='MONTHLY', Q='QUARTERLY', S='SEMI_ANNUALLY', Y='YEARLY'
    ),
    nonTradingDayFillOption=dict(
        N='NON_TRADING_WEEKDAYS', W='NON_TRADING_WEEKDAYS', Weekdays='NON_TRADING_WEEKDAYS',
        C='ALL_CALENDAR_DAYS', A='ALL_CALENDAR_DAYS', All='ALL_CALENDAR_DAYS',
        T='ACTIVE_DAYS_ONLY', Trading='ACTIVE_DAYS_ONLY',
    ),
    nonTradingDayFillMethod=dict(
        C='PREVIOUS_VALUE', P='PREVIOUS_VALUE', Previous='PREVIOUS_VALUE',
        B='NIL_VALUE', Blank='NIL_VALUE', NA='NIL_VALUE',
    ),
    overrideOption=dict(
        A='OVERRIDE_OPTION_GPA', G='OVERRIDE_OPTION_GPA', Average='OVERRIDE_OPTION_GPA',
        C='OVERRIDE_OPTION_CLOSE', Close='OVERRIDE_OPTION_CLOSE',
    ),
    pricingOption=dict(
        P='PRICING_OPTION_PRICE', Price='PRICING_OPTION_PRICE',
        Y='PRICING_OPTION_YIELD', Yield='PRICING_OPTION_YIELD',
    ),
)


def proc_ovrds(**kwargs):
    """
    Bloomberg overrides

    Args:
        **kwargs: overrides

    Returns:
        list of tuples
    """
    return [
        (k, v) for k, v in kwargs.items()
        if k not in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values())
    ]


def proc_elms(**kwargs):
    """
    Bloomberg overrides for elements

    Args:
        **kwargs: overrides

    Returns:
        list of tuples

    Examples:
        >>> proc_elms(PerAdj='A', Per='W')
        [('periodicityAdjustment', 'ACTUAL'), ('periodicitySelection', 'WEEKLY')]
        >>> proc_elms(Days='A', Fill='B')
        [('nonTradingDayFillOption', 'ALL_CALENDAR_DAYS'), ('nonTradingDayFillMethod', 'NIL_VALUE')]
        >>> proc_elms(CshAdjNormal=False, CshAdjAbnormal=True)
        [('adjustmentNormal', False), ('adjustmentAbnormal', True)]
        >>> proc_elms(Per='W', Quote='Average', start_date='2018-01-10')
        [('periodicitySelection', 'WEEKLY'), ('overrideOption', 'OVERRIDE_OPTION_GPA')]
        >>> proc_elms(QuoteType='Y')
        [('pricingOption', 'PRICING_OPTION_YIELD')]
    """
    return [
        (ELEM_KEYS.get(k, k), ELEM_VALS.get(ELEM_KEYS.get(k, k), dict()).get(v, v))
        for k, v in kwargs.items()
        if k in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values())
    ]


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


def hist_file(ticker: str, dt: (str, pd.Timestamp), typ='TRADE'):
    """
    Data file location for Bloomberg historical data

    Args:
        ticker: ticker name
        dt: date
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]

    Returns:
        file location

    Examples:
        >>> data_path = os.environ.get('ROOT_DATA_PATH', '')
        >>> d_file = hist_file(ticker='ES1 Index', dt='2018-08-01')
        >>> root = f'{data_path}/Index/ES1 Index'
        >>> if d_file: assert d_file == f'{root}/TRADE/2018-08-01.parq'
    """
    data_path = os.environ.get('ROOT_DATA_PATH', '')
    if not data_path: return ''
    asset = ticker.split()[-1]
    proper_ticker = ticker.replace('/', '_')
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    return f'{data_path}/{asset}/{proper_ticker}/{typ}/{cur_dt}.parq'


def ref_file(ticker: str, fld: str, has_date=False, from_cache=False, ext='parq', **kwargs):
    """
    Data file location for Bloomberg reference data

    Args:
        ticker: ticker name
        fld: field
        has_date: whether add current date to data file
        from_cache: if has_date is True, whether to load file from latest cached
        ext: file extension
        **kwargs: other overrides passed to ref function

    Returns:
        file location

    Examples:
        >>> data_path = os.environ.get('ROOT_DATA_PATH', '')
        >>> d_file = ref_file('BLT LN Equity', fld='Crncy')
        >>> root = f'{data_path}/Equity/BLT LN Equity'
        >>> if d_file: assert d_file == f'{root}/Crncy/ovrd=None.parq'
    """
    data_path = os.environ.get('ROOT_DATA_PATH', '')
    if not data_path: return ''

    proper_ticker = ticker.replace('/', '_')
    root = f'{data_path}/{ticker.split()[-1]}/{proper_ticker}/{fld}'

    if len(kwargs) > 0: info = utils.to_str(kwargs)[1:-1]
    else: info = 'ovrd=None'

    # Check date info
    if has_date:
        cur_files = []
        if len(kwargs) == 0: missing = f'{root}/asof={utils.cur_time(trading=False)}.{ext}'
        else: missing = f'{root}/asof={utils.cur_time(trading=False)}, {info}.{ext}'
        if from_cache: cur_files = files.all_files(path_name=root, keyword=info, ext=ext)
        if len(cur_files) > 0:
            upd_dt = [val for val in sorted(cur_files)[-1][:-4].split(', ') if 'asof=' in val]
            if len(upd_dt) > 0:
                diff = pd.Timestamp('today') - pd.Timestamp(upd_dt[0].split('=')[-1])
                if diff >= pd.Timedelta('10D'): return missing
            return sorted(cur_files)[-1]
        else: return missing
    else: return f'{root}/{info}.{ext}'


def save_intraday(data: pd.DataFrame, ticker: str, dt: (str, pd.Timestamp), typ='TRADE'):
    """
    Check whether data is done for the day and save

    Args:
        data: data
        ticker: ticker
        dt: date
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
    """
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    logger = logs.get_logger(save_intraday, level='debug')
    info = f'{ticker} / {cur_dt} / {typ}'
    data_file = hist_file(ticker=ticker, dt=dt, typ=typ)
    if not data_file: return

    if data.empty:
        logger.warning(f'data is empty for {info} ...')
        return

    mkt_info = const.market_info(ticker=ticker)
    if 'exch' not in mkt_info:
        logger.error(f'cannot find market info for {ticker} ...')
        return

    exch = mkt_info['exch']
    end_time = pd.Timestamp(
        const.market_timing(ticker=ticker, dt=dt, timing='FINISHED')
    ).tz_localize(exch.tz)
    now = pd.Timestamp('now', tz=exch.tz) - pd.Timedelta('1H')

    if end_time > now:
        logger.debug(f'skip saving cause market close ({end_time}) < now - 1H ({now}) ...')
        return

    logger.info(f'saving data to {data_file} ...')
    files.create_folder(data_file, is_file=True)
    data.to_parquet(data_file)


def query(con, func: str, **kwargs):
    """
    Make Bloomberg query with active connection to save time

    Args:
        con: Bloomberg active connection
        func: function name
        **kwargs: to be passed to query

    Returns:
        pd.DataFrame: query result
    """
    if isinstance(con, pdblp.BCon):
        if con.debug: con.debug = False
        return getattr(con, func)(**kwargs)
    else:
        with pdblp.bopen(port=8194, timeout=5000) as bb:
            return getattr(bb, func)(**kwargs)


def current_missing(**kwargs):
    """
    Check number of trials for missing values

    Returns:
        dict
    """
    data_path = os.environ.get('ROOT_DATA_PATH', '')
    empty_log = f'{data_path}/Logs/EmptyQueries.json'
    if not files.exists(empty_log): return 0
    with open(empty_log, 'r') as fp:
        cur_miss = json.load(fp=fp)

    return cur_miss.get(info_key(**kwargs), 0)


def update_missing(**kwargs):
    """
    Update number of trials for missing values

    Returns:
        dict
    """
    key = info_key(**kwargs)

    data_path = os.environ.get('ROOT_DATA_PATH', '')
    empty_log = f'{data_path}/Logs/EmptyQueries.json'

    cur_miss = dict()
    if files.exists(empty_log):
        with open(empty_log, 'r') as fp:
            cur_miss = json.load(fp=fp)

    cur_miss[key] = cur_miss.get(key, 0) + 1
    while not os.access(empty_log, os.W_OK): time.sleep(1)
    else:
        with open(empty_log, 'w') as fp:
            json.dump(cur_miss, fp=fp, indent=2)

    return cur_miss


def info_key(ticker: str, dt: (str, pd.Timestamp), typ='TRADE', **kwargs):
    """
    Generate key from given info

    Args:
        ticker: ticker name
        dt: date to download
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
        **kwargs: other kwargs

    Returns:
        str
    """
    return utils.to_str(dict(
        ticker=ticker, dt=pd.Timestamp(dt).strftime('%Y-%m-%d'), typ=typ, **kwargs
    ))


def format_earnings(data: pd.DataFrame, header: pd.DataFrame):
    """
    Standardized earning outputs and add percentage by each blocks

    Args:
        data: earning data block
        header: earning headers

    Returns:
        pd.DataFrame
    """
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    res = pd.concat([
        grp.loc[:, ['value']].set_index(header.value)
        for _, grp in data.groupby(data.position)
    ], axis=1)
    res.columns = res.iloc[0]
    res.index.name = None
    res = res.iloc[1:].transpose().reset_index().apply(
        pd.to_numeric, downcast='float', errors='ignore'
    )
    res.rename(columns=lambda vv: '_'.join(vv.split()), inplace=True)

    years = res.columns[res.columns.str.startswith('FY_')]
    lvl_1 = res.Level == 1
    for yr in years:
        res.loc[:, yr] = res.loc[:, yr].round(1)
        pct = f'{yr}_Pct'
        res.loc[:, pct] = 0.
        res.loc[lvl_1, pct] = res.loc[lvl_1, pct].astype(float).round(1)
        res.loc[lvl_1, pct] = res.loc[lvl_1, yr] / res.loc[lvl_1, yr].sum() * 100
        sub_pct = []
        for _, snap in res[::-1].iterrows():
            if snap.Level > 2: continue
            if snap.Level == 1:
                if len(sub_pct) == 0: continue
                sub = pd.concat(sub_pct, axis=1).transpose()
                res.loc[sub.index, pct] = \
                    res.loc[sub.index, yr] / res.loc[sub.index, yr].sum() * 100
                sub_pct = []
            if snap.Level == 2: sub_pct.append(snap)

    res.set_index('Segment_Name', inplace=True)
    res.index.name = None
    return res


def format_dvd(data: pd.DataFrame):
    """
    Generate block data from reference data

    Args:
        data: bulk reference data from Bloomberg

    Returns:
        pd.DataFrame
    """
    if data.empty: return pd.DataFrame()
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    ticker = data.ticker.values[0]
    data = pd.DataFrame(pd.concat([
        grp.loc[:, ['name', 'value']].set_index('name').transpose().reset_index(drop=True)
        for _, grp in data.groupby('position')
    ], sort=False)).reset_index(drop=True).assign(ticker=ticker).set_index('ticker')
    data.columns.name = None
    data.columns = [inflection.underscore(col.replace(' ', '_')) for col in data.columns]
    return data


if __name__ == '__main__':
    """
    CommandLine:
        python -m xbbg.assit all
    """
    import xdoctest
    xdoctest.doctest_module(__file__)
