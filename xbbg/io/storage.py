import pandas as pd

import os

from xbbg import const
from xbbg.io import files, logs
from xbbg.core import utils, overrides

PKG_PATH = files.abspath(__file__, 1)


def bar_file(ticker: str, dt, typ='TRADE') -> str:
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
        >>> bar_file(ticker='ES1 Index', dt='2018-08-01') == ''
        True
        >>> os.environ['BBG_ROOT'] = '/data/bbg'
        >>> bar_file(ticker='ES1 Index', dt='2018-08-01')
        '/data/bbg/Index/ES1 Index/TRADE/2018-08-01.parq'
    """
    data_path = os.environ.get(overrides.BBG_ROOT, '').replace('\\', '/')
    if not data_path: return ''
    asset = ticker.split()[-1]
    proper_ticker = ticker.replace('/', '_')
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    return f'{data_path}/{asset}/{proper_ticker}/{typ}/{cur_dt}.parq'


def ref_file(
        ticker: str, fld: str, has_date=False, cache=False, ext='parq', **kwargs
) -> str:
    """
    Data file location for Bloomberg reference data

    Args:
        ticker: ticker name
        fld: field
        has_date: whether add current date to data file
        cache: if has_date is True, whether to load file from latest cached
        ext: file extension
        **kwargs: other overrides passed to ref function

    Returns:
        str: file location

    Examples:
        >>> import shutil
        >>>
        >>> os.environ['BBG_ROOT'] = ''
        >>> ref_file('BLT LN Equity', fld='Crncy') == ''
        True
        >>> os.environ['BBG_ROOT'] = '/data/bbg'
        >>> ref_file('BLT LN Equity', fld='Crncy', cache=True)
        '/data/bbg/Equity/BLT LN Equity/Crncy/ovrd=None.parq'
        >>> ref_file('BLT LN Equity', fld='Crncy')
        ''
        >>> cur_dt_ = utils.cur_time(tz=utils.DEFAULT_TZ)
        >>> ref_file(
        ...     'BLT LN Equity', fld='DVD_Hist_All', has_date=True, cache=True,
        ... ).replace(cur_dt_, '[cur_date]')
        '/data/bbg/Equity/BLT LN Equity/DVD_Hist_All/asof=[cur_date], ovrd=None.parq'
        >>> ref_file(
        ...     'BLT LN Equity', fld='DVD_Hist_All', has_date=True,
        ...     cache=True, DVD_Start_Dt='20180101',
        ... ).replace(cur_dt_, '[cur_date]')[:-5]
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
        ...     has_date=True, cache=True, ext='pkl'
        ... )
        >>> new_file.split('/')[-1] == f'asof={cur_dt_}, DVD_Start_Dt=20180101.pkl'
        True
        >>> old_file = 'asof=2018-11-02, DVD_Start_Dt=20180101, DVD_End_Dt=20180501.pkl'
        >>> old_full = '/'.join(new_file.split('/')[:-1] + [old_file])
        >>> updated_file = old_full.replace('2018-11-02', cur_dt_)
        >>> updated_file in shutil.copy(old_full, updated_file)
        True
        >>> exist_file = ref_file(
        ...     'AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101',
        ...     has_date=True, cache=True, ext='pkl'
        ... )
        >>> exist_file == updated_file
        False
        >>> exist_file = ref_file(
        ...     'AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101',
        ...     DVD_End_Dt='20180501', has_date=True, cache=True, ext='pkl'
        ... )
        >>> exist_file == updated_file
        True
    """
    data_path = os.environ.get(overrides.BBG_ROOT, '').replace('\\', '/')
    if (not data_path) or (not cache): return ''

    proper_ticker = ticker.replace('/', '_')
    cache_days = kwargs.pop('cache_days', 10)
    root = f'{data_path}/{ticker.split()[-1]}/{proper_ticker}/{fld}'

    ref_kw = {k: v for k, v in kwargs.items() if k not in overrides.PRSV_COLS}
    if len(ref_kw) > 0: info = utils.to_str(ref_kw)[1:-1].replace('|', '_')
    else: info = 'ovrd=None'

    # Check date info
    if has_date:
        cache_file = f'{root}/asof=[cur_date], {info}.{ext}'
        cur_dt = utils.cur_time()
        start_dt = pd.date_range(end=cur_dt, freq=f'{cache_days}D', periods=2)[0]
        for dt in pd.date_range(start=start_dt, end=cur_dt, normalize=True)[1:][::-1]:
            cur_file = cache_file.replace('[cur_date]', dt.strftime("%Y-%m-%d"))
            if files.exists(cur_file): return cur_file
        return cache_file.replace('[cur_date]', cur_dt)

    return f'{root}/{info}.{ext}'


def save_intraday(data: pd.DataFrame, ticker: str, dt, typ='TRADE', **kwargs):
    """
    Check whether data is done for the day and save

    Args:
        data: data
        ticker: ticker
        dt: date
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]

    Examples:
        >>> os.environ['BBG_ROOT'] = f'{PKG_PATH}/tests/data'
        >>> sample = pd.read_parquet(f'{PKG_PATH}/tests/data/aapl.parq')
        >>> save_intraday(sample, 'AAPL US Equity', '2018-11-02')
        >>> # Invalid exchange
        >>> save_intraday(sample, 'AAPL XX Equity', '2018-11-02')
        >>> # Invalid empty data
        >>> save_intraday(pd.DataFrame(), 'AAPL US Equity', '2018-11-02')
        >>> # Invalid date - too close
        >>> cur_dt_ = utils.cur_time()
        >>> save_intraday(sample, 'AAPL US Equity', cur_dt_)
    """
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    logger = logs.get_logger(save_intraday, **kwargs)
    info = f'{ticker} / {cur_dt} / {typ}'
    data_file = bar_file(ticker=ticker, dt=dt, typ=typ)
    if not data_file: return

    if data.empty:
        logger.warning(f'data is empty for {info} ...')
        return

    exch = const.exch_info(ticker=ticker, **kwargs)
    if exch.empty: return

    end_time = pd.Timestamp(
        const.market_timing(ticker=ticker, dt=dt, timing='FINISHED', **kwargs)
    ).tz_localize(exch.tz)
    now = pd.Timestamp('now', tz=exch.tz) - pd.Timedelta('1H')

    if end_time > now:
        logger.debug(f'skip saving cause market close ({end_time}) < now - 1H ({now}) ...')
        return

    logger.info(f'saving data to {data_file} ...')
    files.create_folder(data_file, is_file=True)
    data.to_parquet(data_file)
