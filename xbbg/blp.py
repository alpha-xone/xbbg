import pandas as pd

from functools import partial
from itertools import product
from contextlib import contextmanager

from xbbg import __version__, const, pipeline
from xbbg.io import logs, files, storage
from xbbg.core import utils, conn, process
from xbbg.core.conn import connect

__all__ = [
    '__version__',
    'connect',
    'bdp',
    'bds',
    'bdh',
    'bdib',
    'bdtick',
    'earning',
    'dividend',
    'beqs',
    'live',
    'subscribe',
    'adjust_ccy',
    'turnover',
]


def bdp(tickers, flds, **kwargs) -> pd.DataFrame:
    """
    Bloomberg reference data

    Args:
        tickers: tickers
        flds: fields to query
        **kwargs: Bloomberg overrides

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(bdp, **kwargs)

    if isinstance(tickers, str): tickers = [tickers]
    if isinstance(flds, str): flds = [flds]

    request = process.create_request(
        service='//blp/refdata',
        request='ReferenceDataRequest',
        **kwargs,
    )
    process.init_request(request=request, tickers=tickers, flds=flds, **kwargs)
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'field']):
        return pd.DataFrame()

    return (
        res
        .set_index(['ticker', 'field'])
        .unstack(level=1)
        .rename_axis(index=None, columns=[None, None])
        .droplevel(axis=1, level=0)
        .loc[:, res.field.unique()]
        .pipe(pipeline.standard_cols, col_maps=kwargs.get('col_maps', None))
    )


def bds(tickers, flds, use_port=False, **kwargs) -> pd.DataFrame:
    """
    Bloomberg block data

    Args:
        tickers: ticker(s)
        flds: field
        use_port: use `PortfolioDataRequest`
        **kwargs: other overrides for query

    Returns:
        pd.DataFrame: block data
    """
    logger = logs.get_logger(bds, **kwargs)

    part = partial(_bds_, fld=flds, logger=logger, use_port=use_port, **kwargs)
    if isinstance(tickers, str): tickers = [tickers]
    return pd.DataFrame(pd.concat(map(part, tickers), sort=False))


def _bds_(
        ticker: str,
        fld: str,
        logger: logs.logging.Logger,
        use_port: bool = False,
        **kwargs,
) -> pd.DataFrame:
    """
    Get data of BDS of single ticker
    """
    if 'has_date' not in kwargs: kwargs['has_date'] = True
    data_file = storage.ref_file(ticker=ticker, fld=fld, ext='pkl', **kwargs)
    if files.exists(data_file):
        logger.debug(f'Loading Bloomberg data from: {data_file}')
        return pd.DataFrame(pd.read_pickle(data_file))

    request = process.create_request(
        service='//blp/refdata',
        request='PortfolioDataRequest' if use_port else 'ReferenceDataRequest',
        **kwargs,
    )
    process.init_request(request=request, tickers=ticker, flds=fld, **kwargs)
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'field']):
        return pd.DataFrame()

    data = (
        res
        .set_index(['ticker', 'field'])
        .droplevel(axis=0, level=1)
        .rename_axis(index=None)
        .pipe(pipeline.standard_cols, col_maps=kwargs.get('col_maps', None))
    )
    if data_file:
        logger.debug(f'Saving Bloomberg data to: {data_file}')
        files.create_folder(data_file, is_file=True)
        data.to_pickle(data_file)

    return data


def bdh(
        tickers, flds=None, start_date=None, end_date='today', adjust=None, **kwargs
) -> pd.DataFrame:
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
    """
    logger = logs.get_logger(bdh, **kwargs)

    if flds is None: flds = ['Last_Price']
    e_dt = utils.fmt_dt(end_date, fmt='%Y%m%d')
    if start_date is None: start_date = pd.Timestamp(e_dt) - pd.Timedelta(weeks=8)
    s_dt = utils.fmt_dt(start_date, fmt='%Y%m%d')

    request = process.create_request(
        service='//blp/refdata',
        request='HistoricalDataRequest',
        **kwargs,
    )
    process.init_request(
        request=request, tickers=tickers, flds=flds,
        start_date=s_dt, end_date=e_dt, adjust=adjust, **kwargs
    )
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(process.process_hist, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'date']):
        return pd.DataFrame()

    return (
        res
        .set_index(['ticker', 'date'])
        .unstack(level=0)
        .rename_axis(index=None, columns=[None, None])
        .swaplevel(0, 1, axis=1)
        .reindex(columns=utils.flatten(tickers), level=0)
        .reindex(columns=utils.flatten(flds), level=1)
    )


def bdib(ticker: str, dt, session='allday', typ='TRADE', **kwargs) -> pd.DataFrame:
    """
    Bloomberg intraday bar data

    Args:
        ticker: ticker name
        dt: date to download
        session: [allday, day, am, pm, pre, post]
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
        **kwargs:
            ref: reference ticker or exchange
                 used as supplement if exchange info is not defined for `ticker`
            batch: whether is batch process to download data
            log: level of logs

    Returns:
        pd.DataFrame
    """
    from xbbg.core import trials

    logger = logs.get_logger(bdib, **kwargs)

    ex_info = const.exch_info(ticker=ticker, **kwargs)
    if ex_info.empty: raise KeyError(f'Cannot find exchange info for {ticker}')

    ss_rng = process.time_range(dt=dt, ticker=ticker, session=session, tz=ex_info.tz, **kwargs)
    data_file = storage.bar_file(ticker=ticker, dt=dt, typ=typ)
    if files.exists(data_file) and kwargs.get('cache', True) and (not kwargs.get('reload', False)):
        res = (
            pd.read_parquet(data_file)
            .pipe(pipeline.add_ticker, ticker=ticker)
            .loc[ss_rng[0]:ss_rng[1]]
        )
        if not res.empty:
            logger.debug(f'Loading Bloomberg intraday data from: {data_file}')
            return res

    if not process.check_current(dt=dt, logger=logger, **kwargs): return pd.DataFrame()

    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    q_tckr = ticker
    if ex_info.get('is_fut', False):
        is_sprd = ex_info.get('has_sprd', False) and (len(ticker[:-1]) != ex_info['tickers'][0])
        if not is_sprd:
            q_tckr = fut_ticker(gen_ticker=ticker, dt=dt, freq=ex_info['freq'])
            if q_tckr == '':
                logger.error(f'cannot find futures ticker for {ticker} ...')
                return pd.DataFrame()

    info_log = f'{q_tckr} / {cur_dt} / {typ}'
    trial_kw = dict(ticker=ticker, dt=dt, typ=typ, func='bdib')
    num_trials = trials.num_trials(**trial_kw)
    if num_trials >= 2:
        if kwargs.get('batch', False): return pd.DataFrame()
        logger.info(f'{num_trials} trials with no data {info_log}')
        return pd.DataFrame()

    while conn.bbg_session(**kwargs).tryNextEvent(): pass
    time_rng = process.time_range(dt=dt, ticker=ticker, session='allday', **kwargs)
    request = process.create_request(
        service='//blp/refdata',
        request='IntradayBarRequest',
        settings=[
            ('security', ticker),
            ('eventType', typ),
            ('interval', kwargs.get('interval', 1)),
            ('startDateTime', time_rng[0]),
            ('endDateTime', time_rng[1]),
        ],
        **kwargs,
    )
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(func=process.process_bar, **kwargs))
    if res.empty or ('time' not in res):
        logger.warning(f'No data for {info_log} ...')
        trials.update_trials(cnt=num_trials + 1, **trial_kw)
        return pd.DataFrame()

    data = (
        res
        .set_index('time')
        .rename_axis(index=None)
        .rename(columns={'numEvents': 'num_trds'})
        .tz_localize('UTC')
        .tz_convert(ex_info.tz)
        .pipe(pipeline.add_ticker, ticker=ticker)
    )
    if kwargs.get('cache', True):
        storage.save_intraday(data=data[ticker], ticker=ticker, dt=dt, typ=typ, **kwargs)

    return data.loc[ss_rng[0]:ss_rng[1]]


def bdtick(ticker, dt, session='allday', time_range=None, types=None, **kwargs) -> pd.DataFrame:
    """
    Bloomberg tick data

    Args:
        ticker: ticker name
        dt: date to download
        session: [allday, day, am, pm, pre, post]
        time_range: tuple of start and end time (must be converted into UTC)
                    if this is given, `dt` and `session` will be ignored
        types: str or list, one or combinations of [
            TRADE, AT_TRADE, BID, ASK, MID_PRICE,
            BID_BEST, ASK_BEST, BEST_BID, BEST_ASK,
        ]

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(bdtick, **kwargs)

    if types is None: types = ['TRADE']
    exch = const.exch_info(ticker=ticker, **kwargs)
    if exch.empty: raise LookupError(f'Cannot find exchange info for {ticker}')

    if isinstance(time_range, (tuple, list)) and (len(time_range) == 2):
        cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
        time_rng = (
            pd.DatetimeIndex([
                f'{cur_dt} {time_range[0]}',
                f'{cur_dt} {time_range[1]}',
            ])
            .tz_localize(exch.tz)
            .tz_convert(process.DEFAULT_TZ)
            .tz_convert('UTC')
        )
    else:
        time_rng = process.time_range(dt=dt, ticker=ticker, session=session, **kwargs)

    while conn.bbg_session(**kwargs).tryNextEvent(): pass
    request = process.create_request(
        service='//blp/refdata',
        request='IntradayTickRequest',
        settings=[
            ('security', ticker),
            ('startDateTime', time_rng[0]),
            ('endDateTime', time_rng[1]),
            ('includeConditionCodes', True),
            ('includeExchangeCodes', True),
            ('includeNonPlottableEvents', True),
            ('includeBrokerCodes', True),
            ('includeRpsCodes', True),
            ('includeTradeTime', True),
            ('includeActionCodes', True),
            ('includeIndicatorCodes', True),
        ],
        append={'eventTypes': types},
        **kwargs,
    )

    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request)

    res = pd.DataFrame(process.rec_events(func=process.process_bar, typ='t', **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or ('time' not in res): return pd.DataFrame()

    return (
        res
        .set_index('time')
        .rename_axis(index=None)
        .tz_localize('UTC')
        .tz_convert(exch.tz)
        .pipe(pipeline.add_ticker, ticker=ticker)
        .rename(columns={
            'size': 'volume',
            'type': 'typ',
            'conditionCodes': 'cond',
            'exchangeCode': 'exch',
            'tradeTime': 'trd_time',
        })
    )


def earning(ticker, by='Geo', typ='Revenue', ccy=None, level=None, **kwargs) -> pd.DataFrame:
    """
    Earning exposures by Geo or Products

    Args:
        ticker: ticker name
        by: [G(eo), P(roduct)]
        typ: type of earning, start with `PG_` in Bloomberg FLDS - default `Revenue`
            `Revenue` - Revenue of the company
            `Operating_Income` - Operating Income (also named as EBIT) of the company
            `Assets` - Assets of the company
            `Gross_Profit` - Gross profit of the company
            `Capital_Expenditures` - Capital expenditures of the company
        ccy: currency of earnings
        level: hierarchy level of earnings

    Returns:
        pd.DataFrame
    """
    kwargs.pop('raw', None)
    ovrd = 'G' if by[0].upper() == 'G' else 'P'
    new_kw = dict(Product_Geo_Override=ovrd)

    year = kwargs.pop('year', None)
    periods = kwargs.pop('periods', None)
    if year: kwargs['Eqy_Fund_Year'] = year
    if periods: kwargs['Number_Of_Periods'] = periods

    header = bds(tickers=ticker, flds='PG_Bulk_Header', **new_kw, **kwargs)
    if ccy: kwargs['Eqy_Fund_Crncy'] = ccy
    if level: kwargs['PG_Hierarchy_Level'] = level
    data = bds(tickers=ticker, flds=f'PG_{typ}', **new_kw, **kwargs)

    if data.empty or header.empty: return pd.DataFrame()
    if data.shape[1] != header.shape[1]:
        raise ValueError('Inconsistent shape of data and header')
    data.columns = (
        header.iloc[0]
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('_20', '20')
        .tolist()
    )

    if 'level' not in data: raise KeyError('Cannot find [level] in data')
    for yr in data.columns[data.columns.str.startswith('fy')]:
        process.earning_pct(data=data, yr=yr)

    return data


def dividend(tickers, typ='all', start_date=None, end_date=None, **kwargs) -> pd.DataFrame:
    """
    Bloomberg dividend / split history

    Args:
        tickers: list of tickers
        typ: dividend adjustment type
            `all`:       `DVD_Hist_All`
            `dvd`:       `DVD_Hist`
            `split`:     `Eqy_DVD_Hist_Splits`
            `gross`:     `Eqy_DVD_Hist_Gross`
            `adjust`:    `Eqy_DVD_Adjust_Fact`
            `adj_fund`:  `Eqy_DVD_Adj_Fund`
            `with_amt`:  `DVD_Hist_All_with_Amt_Status`
            `dvd_amt`:   `DVD_Hist_with_Amt_Status`
            `gross_amt`: `DVD_Hist_Gross_with_Amt_Stat`
            `projected`: `BDVD_Pr_Ex_Dts_DVD_Amts_w_Ann`
        start_date: start date
        end_date: end date
        **kwargs: overrides

    Returns:
        pd.DataFrame
    """
    kwargs.pop('raw', None)
    if isinstance(tickers, str): tickers = [tickers]
    tickers = [t for t in tickers if ('Equity' in t) and ('=' not in t)]

    fld = const.DVD_TPYES.get(typ, typ)

    if (fld == 'Eqy_DVD_Adjust_Fact') and ('Corporate_Actions_Filter' not in kwargs):
        kwargs['Corporate_Actions_Filter'] = 'NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE'

    if start_date:
        kwargs['DVD_Start_Dt'] = utils.fmt_dt(start_date, fmt='%Y%m%d')
    if end_date:
        kwargs['DVD_End_Dt'] = utils.fmt_dt(end_date, fmt='%Y%m%d')

    return bds(tickers=tickers, flds=fld, col_maps=const.DVD_COLS, **kwargs)


def beqs(screen, asof=None, typ='PRIVATE', group='General', **kwargs) -> pd.DataFrame:
    """
    Bloomberg equity screening

    Args:
        screen: screen name
        asof: as of date
        typ: GLOBAL/B (Bloomberg) or PRIVATE/C (Custom, default)
        group: group name if screen is organized into groups

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(beqs, **kwargs)

    request = process.create_request(
        service='//blp/refdata',
        request='BeqsRequest',
        settings=[
            ('screenName', screen),
            ('screenType', 'GLOBAL' if typ[0].upper() in ['G', 'B'] else 'PRIVATE'),
            ('Group', group),
        ],
        ovrds=[('PiTDate', utils.fmt_dt(asof, '%Y%m%d'))] if asof else None,
        **kwargs,
    )

    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)
    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if res.empty:
        if kwargs.get('trial', 0): return pd.DataFrame()
        return beqs(screen=screen, asof=asof, typ=typ, group=group, trial=1, **kwargs)

    if kwargs.get('raw', False): return res
    cols = res.field.unique()
    return (
        res
        .set_index(['ticker', 'field'])
        .unstack(level=1)
        .rename_axis(index=None, columns=[None, None])
        .droplevel(axis=1, level=0)
        .loc[:, cols]
        .pipe(pipeline.standard_cols)
    )


@contextmanager
def subscribe(tickers, flds=None, identity=None, options=None, **kwargs):
    """
    Subscribe Bloomberg realtime data

    Args:
        tickers: list of tickers
        flds: fields to subscribe, default: Last_Price, Bid, Ask
        identity: Bloomberg identity
    """
    logger = logs.get_logger(subscribe, **kwargs)
    if isinstance(tickers, str): tickers = [tickers]
    if flds is None: flds = ['Last_Price', 'Bid', 'Ask']
    if isinstance(flds, str): flds = [flds]

    sub_list = conn.blpapi.SubscriptionList()
    for ticker in tickers:
        topic = f'//blp/mktdata/{ticker}'
        cid = conn.blpapi.CorrelationId(ticker)
        logger.debug(f'Subscribing {cid} => {topic}')
        sub_list.add(topic, flds, correlationId=cid, options=options)

    try:
        conn.bbg_session(**kwargs).subscribe(sub_list, identity)
        yield
    finally:
        conn.bbg_session(**kwargs).unsubscribe(sub_list)


async def live(tickers, flds=None, info=None, max_cnt=0, options=None, **kwargs):
    """
    Subscribe and getting data feeds from

    Args:
        tickers: list of tickers
        flds: fields to subscribe
        info: list of keys of interests (ticker will be included)
        max_cnt: max number of data points to receive

    Yields:
        dict: Bloomberg market data

    Examples:
        >>> # async for _ in live('SPY US Equity', info=const.LIVE_INFO): pass
    """
    from collections.abc import Iterable

    logger = logs.get_logger(live, **kwargs)
    evt_typs = conn.event_types()

    if flds is None:
        s_flds = ['LAST_PRICE', 'BID', 'ASK']
    else:
        if isinstance(flds, str): flds = [flds]
        s_flds = [fld.upper() for fld in flds]

    if isinstance(info, str): info = [info]
    if isinstance(info, Iterable): info = [key.upper() for key in info]
    if info is None: info = const.LIVE_INFO

    sess = conn.bbg_session(**kwargs)
    while sess.tryNextEvent(): pass
    with subscribe(tickers=tickers, flds=s_flds, options=options, **kwargs):
        cnt = 0
        while True and cnt <= max_cnt:
            try:
                ev = sess.tryNextEvent()
                if ev is None: continue
                if evt_typs[ev.eventType()] != 'SUBSCRIPTION_DATA': continue

                for msg, fld in product(ev, s_flds):
                    if not msg.hasElement(fld): continue
                    if msg.getElement(fld).isNull(): continue
                    yield {
                        **{
                            'TICKER': msg.correlationIds()[0].value(),
                            'FIELD': fld,
                        },
                        **{
                            str(elem.name()): process.elem_value(elem)
                            for elem in msg.asElement().elements()
                            if (True if not info else str(elem.name()) in info)
                        },
                    }
                    if max_cnt: cnt += 1

            except ValueError as e: logger.debug(e)
            except KeyboardInterrupt: break


def active_futures(ticker: str, dt, **kwargs) -> str:
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
    fut_2 = fut_ticker(gen_ticker=f2, dt=dt, freq=info.get('freq', 'M'), **kwargs)
    fut_1 = fut_ticker(gen_ticker=f1, dt=dt, freq=info.get('freq', 'M'), **kwargs)

    fut_tk = bdp(tickers=[fut_1, fut_2], flds='Last_Tradeable_Dt')

    if pd.Timestamp(dt).month < pd.Timestamp(fut_tk.last_tradeable_dt[0]).month: return fut_1

    dts = pd.bdate_range(end=dt, periods=10)
    volume = bdh(fut_tk.index, flds='volume', start_date=dts[0], end_date=dts[-1])
    if volume.empty: return fut_1
    return volume.iloc[-1].idxmax()[0]


def fut_ticker(gen_ticker: str, dt, freq: str, **kwargs) -> str:
    """
    Get proper ticker from generic ticker

    Args:
        gen_ticker: generic ticker
        dt: date
        freq: futures contract frequency

    Returns:
        str: exact futures ticker
    """
    logger = logs.get_logger(fut_ticker, **kwargs)
    dt = pd.Timestamp(dt)
    t_info = gen_ticker.split()
    pre_dt = pd.bdate_range(end='today', periods=1)[-1]
    same_month = (pre_dt.month == dt.month) and (pre_dt.year == dt.year)

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
    months = pd.date_range(start=dt, periods=max(idx + month_ext, 3), freq=freq)
    logger.debug(f'pulling expiry dates for months: {months}')

    def to_fut(month):
        return prefix + const.Futures[month.strftime('%b')] + \
            month.strftime('%y')[-1 if same_month else -2:] + ' ' + postfix

    fut = [to_fut(m) for m in months]
    logger.debug(f'trying futures: {fut}')
    # noinspection PyBroadException
    try:
        fut_matu = bdp(tickers=fut, flds='last_tradeable_dt')
    except Exception as e1:
        logger.error(f'error downloading futures contracts (1st trial) {e1}:\n{fut}')
        # noinspection PyBroadException
        try:
            fut = fut[:-1]
            logger.debug(f'trying futures (2nd trial): {fut}')
            fut_matu = bdp(tickers=fut, flds='last_tradeable_dt')
        except Exception as e2:
            logger.error(f'error downloading futures contracts (2nd trial) {e2}:\n{fut}')
            return ''

    if 'last_tradeable_dt' not in fut_matu:
        logger.warning(f'no futures found for {fut}')
        return ''

    fut_matu.sort_values(by='last_tradeable_dt', ascending=True, inplace=True)
    sub_fut = fut_matu[pd.DatetimeIndex(fut_matu.last_tradeable_dt) > dt]
    logger.debug(f'futures full chain:\n{fut_matu.to_string()}')
    logger.debug(f'getting index {idx} from:\n{sub_fut.to_string()}')
    return sub_fut.index.values[idx]


def adjust_ccy(data: pd.DataFrame, ccy: str = 'USD') -> pd.DataFrame:
    """
    Adjust

    Args:
        data: daily price / turnover / etc. to adjust
        ccy: currency to adjust to

    Returns:
        pd.DataFrame
    """
    if data.empty: return pd.DataFrame()
    if ccy.lower() == 'local': return data
    tickers = data.columns.get_level_values(level=0).unique()
    start_date = data.index[0]
    end_date = data.index[-1]

    uccy = bdp(tickers=tickers, flds='crncy')
    if not uccy.empty:
        adj = (
            uccy.crncy
            .map(lambda v: {
                'ccy': None if v.upper() == ccy else f'{ccy}{v.upper()} Curncy',
                'factor': 100. if v[-1].islower() else 1.,
            })
            .apply(pd.Series)
            .dropna(subset=['ccy'])
        )
    else: adj = pd.DataFrame()

    if not adj.empty:
        fx = (
            bdh(tickers=adj.ccy.unique(), start_date=start_date, end_date=end_date)
            .xs('Last_Price', axis=1, level=1)
        )
    else: fx = pd.DataFrame()

    return (
        pd.concat([
            pd.Series(
                (
                    data[t]
                    .dropna()
                    .prod(axis=1)
                    .div(
                        (fx[adj.loc[t, 'ccy']] * adj.loc[t, 'factor'])
                        if t in adj.index else 1.,
                    )
                ),
                name=t,
            )
            for t in tickers
        ], axis=1)
    )


def turnover(
        tickers,
        flds='Turnover',
        start_date=None,
        end_date=None,
        ccy: str = 'USD',
        factor: float = 1e6,
) -> pd.DataFrame:
    """
    Currency adjusted turnover (in million)

    Args:
        tickers: ticker or list of tickers
        flds: override `flds`,
        start_date: start date, default 1 month prior to `end_date`
        end_date: end date, default T - 1
        ccy: currency - 'USD' (default), any currency, or 'local' (no adjustment)
        factor: adjustment factor, default 1e6 - return values in millions

    Returns:
        pd.DataFrame
    """
    if end_date is None:
        end_date = pd.bdate_range(end='today', periods=2)[0]
    if start_date is None:
        start_date = pd.bdate_range(end=end_date, periods=2, freq='M')[0]
    if isinstance(tickers, str): tickers = [tickers]

    data = bdh(tickers=tickers, flds=flds, start_date=start_date, end_date=end_date)
    cols = data.columns.get_level_values(level=0).unique()

    # If turnover is not available, use volume and vwap for calculation
    use_volume = pd.DataFrame()
    if isinstance(flds, str) and (flds.lower() == 'turnover'):
        vol_tcks = [t for t in tickers if t not in cols]
        if vol_tcks:
            use_volume = turnover(
                tickers=vol_tcks,
                flds=['eqy_weighted_avg_px', 'volume'],
                start_date=start_date,
                end_date=end_date,
                ccy=ccy,
                factor=factor,
            )

    if data.empty and use_volume.empty: return pd.DataFrame()
    return pd.concat([adjust_ccy(data=data, ccy=ccy).div(factor), use_volume], axis=1)
