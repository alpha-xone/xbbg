import pandas as pd

import os

from xbbg.core import utils, const
from xbbg.io import files, logs
from xbbg.core.timezone import DEFAULT_TZ

# Set os.environ['BBG_ROOT'] = '/your/bbg/data/path'
#     to enable xbbg saving data locally
BBG_ROOT = 'BBG_ROOT'

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

    Examples:
        >>> proc_ovrds(DVD_Start_Dt='20180101')
        [('DVD_Start_Dt', '20180101')]
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


def hist_file(ticker: str, dt, typ='TRADE'):
    """
    Data file location for Bloomberg historical data

    Args:
        ticker: ticker name
        dt: date
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]

    Returns:
        file location

    Examples:
        >>> os.environ['BBG_ROOT'] = ''
        >>> hist_file(ticker='ES1 Index', dt='2018-08-01') == ''
        True
        >>> os.environ['BBG_ROOT'] = '/data/bbg'
        >>> hist_file(ticker='ES1 Index', dt='2018-08-01')
        '/data/bbg/Index/ES1 Index/TRADE/2018-08-01.parq'
    """
    data_path = os.environ.get(BBG_ROOT, '')
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
        >>> import shutil
        >>>
        >>> os.environ['BBG_ROOT'] = ''
        >>> ref_file('BLT LN Equity', fld='Crncy') == ''
        True
        >>> os.environ['BBG_ROOT'] = '/data/bbg'
        >>> ref_file('BLT LN Equity', fld='Crncy')
        '/data/bbg/Equity/BLT LN Equity/Crncy/ovrd=None.parq'
        >>> cur_dt = utils.cur_time(tz=DEFAULT_TZ)
        >>> ref_file(
        ...     'BLT LN Equity', fld='DVD_Hist_All', has_date=True
        ... ).replace(cur_dt, '[cur_date]')
        '/data/bbg/Equity/BLT LN Equity/DVD_Hist_All/asof=[cur_date], ovrd=None.parq'
        >>> ref_file(
        ...     'BLT LN Equity', fld='DVD_Hist_All', has_date=True, DVD_Start_Dt='20180101'
        ... ).replace(cur_dt, '[cur_date]')[:-5]
        '/data/bbg/Equity/BLT LN Equity/DVD_Hist_All/asof=[cur_date], DVD_Start_Dt=20180101'
        >>> sample = 'asof=2018-11-02, DVD_Start_Dt=20180101, DVD_End_Dt=20180501.pkl'
        >>> root_path = 'xbbg/tests/data'
        >>> sub_path = f'{root_path}/Equity/AAPL US Equity/DVD_Hist_All'
        >>> os.environ['BBG_ROOT'] = root_path
        >>> for tmp_file in files.all_files(sub_path): os.remove(tmp_file)
        >>> files.create_folder(sub_path)
        >>> sample in shutil.copy(f'{root_path}/{sample}', sub_path)
        True
        >>> new_file = ref_file(
        ...     'AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101',
        ...     has_date=True, from_cache=True, ext='pkl'
        ... )
        >>> new_file.split('/')[-1] == f'asof={cur_dt}, DVD_Start_Dt=20180101.pkl'
        True
        >>> old_file = 'asof=2018-11-02, DVD_Start_Dt=20180101, DVD_End_Dt=20180501.pkl'
        >>> old_full = '/'.join(new_file.split('/')[:-1] + [old_file])
        >>> updated_file = old_full.replace('2018-11-02', cur_dt)
        >>> updated_file in shutil.copy(old_full, updated_file)
        True
        >>> exist_file = ref_file(
        ...     'AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101',
        ...     has_date=True, from_cache=True, ext='pkl'
        ... )
        >>> exist_file == updated_file
        True
    """
    data_path = os.environ.get(BBG_ROOT, '')
    if not data_path: return ''

    proper_ticker = ticker.replace('/', '_')
    root = f'{data_path}/{ticker.split()[-1]}/{proper_ticker}/{fld}'

    if len(kwargs) > 0: info = utils.to_str(kwargs)[1:-1]
    else: info = 'ovrd=None'

    # Check date info
    if has_date:
        cur_files = []
        cur_dt = utils.cur_time()

        if from_cache:
            cur_files = files.all_files(path_name=root, keyword=info, ext=ext)

        missing = f'{root}/asof={cur_dt}, {info}.{ext}'
        if len(cur_files) > 0:
            upd_dt = [val for val in sorted(cur_files)[-1][:-4].split(', ') if 'asof=' in val]
            if len(upd_dt) > 0:
                diff = pd.Timestamp('today') - pd.Timestamp(upd_dt[0].split('=')[-1])
                if diff >= pd.Timedelta('10D'): return missing
            return sorted(cur_files)[-1]
        else: return missing

    else: return f'{root}/{info}.{ext}'


def save_intraday(data: pd.DataFrame, ticker: str, dt, typ='TRADE'):
    """
    Check whether data is done for the day and save

    Args:
        data: data
        ticker: ticker
        dt: date
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]

    Examples:
        >>> os.environ['BBG_ROOT'] = 'xbbg/tests/data'
        >>> sample = pd.read_parquet('xbbg/tests/data/aapl.parq')
        >>> save_intraday(sample, 'AAPL US Equity', '2018-11-02')
        >>> # Invalid exchange
        >>> save_intraday(sample, 'AAPL XX Equity', '2018-11-02')
        >>> # Invalid empty data
        >>> save_intraday(pd.DataFrame(), 'AAPL US Equity', '2018-11-02')
        >>> # Invalid date - too close
        >>> cur_dt = utils.cur_time()
        >>> save_intraday(sample, 'AAPL US Equity', cur_dt)
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


def info_key(ticker: str, dt, typ='TRADE', **kwargs):
    """
    Generate key from given info

    Args:
        ticker: ticker name
        dt: date to download
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
        **kwargs: other kwargs

    Returns:
        str

    Examples:
        >>> info_key('X US Equity', '2018-02-01', OtherInfo='Any')
        '{ticker=X US Equity, dt=2018-02-01, typ=TRADE, OtherInfo=Any}'
    """
    return utils.to_str(dict(
        ticker=ticker, dt=pd.Timestamp(dt).strftime('%Y-%m-%d'), typ=typ, **kwargs
    ))


def format_earning(data: pd.DataFrame, header: pd.DataFrame):
    """
    Standardized earning outputs and add percentage by each blocks

    Args:
        data: earning data block
        header: earning headers

    Returns:
        pd.DataFrame

    Examples:
        >>> format_earning(
        ...     data=pd.read_pickle('xbbg/tests/data/sample_earning.pkl'),
        ...     header=pd.read_pickle('xbbg/tests/data/sample_earning_header.pkl')
        ... ).round(2)
                         Level  FY_2017  FY_2017_Pct
        Asia-Pacific       1.0   3540.0        66.43
           China           2.0   1747.0        49.35
           Japan           2.0   1242.0        35.08
           Singapore       2.0    551.0        15.56
        United States      1.0   1364.0        25.60
        Europe             1.0    263.0         4.94
        Other Countries    1.0    162.0         3.04
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

    Examples:
        >>> res = format_dvd(
        ...     data=pd.read_pickle('xbbg/tests/data/sample_dvd.pkl')
        ... ).loc[:, ['ex_date', 'rec_date', 'dvd_amt']].round(2)
        >>> res.index.name = None
        >>> res
                        ex_date    rec_date  dvd_amt
        C US Equity  2018-02-02  2018-02-05     0.32
    """
    if data.empty: return pd.DataFrame()
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    col_maps = {
        'Declared Date': 'dec_date', 'Ex-Date': 'ex_date',
        'Record Date': 'rec_date', 'Payable Date': 'pay_date',
        'Dividend Amount': 'dvd_amt', 'Dividend Frequency': 'dvd_freq',
        'Dividend Type': 'dvd_type'
    }

    ticker = data.ticker.values[0]
    data = pd.DataFrame(pd.concat([
        grp.loc[:, ['name', 'value']].set_index('name').transpose().reset_index(drop=True)
        for _, grp in data.groupby('position')
    ], sort=False)).reset_index(drop=True)\
        .assign(ticker=ticker).set_index('ticker').rename(columns=col_maps)
    data.columns.name = None

    if 'dvd_amt' in data.columns:
        data.loc[:, 'dvd_amt'] = data.loc[:, 'dvd_amt'].apply(pd.to_numeric, errors='ignore')

    return data
