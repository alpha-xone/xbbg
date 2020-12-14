import pandas as pd
import numpy as np

from contextlib import contextmanager

from xbbg import __version__, const, pipeline
from xbbg.io import logs, files, storage
from xbbg.core import utils, conn, process

__all__ = [
    '__version__',
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

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('ReferenceDataRequest')

    process.init_request(request=request, tickers=tickers, flds=flds, **kwargs)
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'field']):
        return pd.DataFrame()

    col_maps = kwargs.get('col_maps', None)
    cols = res.field.unique()
    return (
        res
        .set_index(['ticker', 'field'])
        .unstack(level=1)
        .rename_axis(index=None, columns=[None, None])
        .droplevel(axis=1, level=0)
        .loc[:, cols]
        .pipe(pipeline.standard_cols, col_maps=col_maps)
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

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest(
        'PortfolioDataRequest' if use_port else 'ReferenceDataRequest'
    )

    if isinstance(tickers, str):
        if 'has_date' not in kwargs: kwargs['has_date'] = True
        data_file = storage.ref_file(
            ticker=tickers, fld=flds, ext='pkl', **kwargs
        )
        if files.exists(data_file):
            logger.debug(f'Loading Bloomberg data from: {data_file}')
            return pd.DataFrame(pd.read_pickle(data_file))

        process.init_request(request=request, tickers=tickers, flds=flds, **kwargs)
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

    return pd.DataFrame(pd.concat([
        bds(tickers=ticker, flds=flds, **kwargs) for ticker in tickers
    ], sort=False))


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

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('HistoricalDataRequest')

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


def bdib(
        ticker: str, dt, session='allday', typ='TRADE', **kwargs
) -> pd.DataFrame:
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

    ss_rng = process.time_range(
        dt=dt, ticker=ticker, session=session, tz=ex_info.tz, **kwargs
    )
    data_file = storage.bar_file(ticker=ticker, dt=dt, typ=typ)
    if files.exists(data_file) and kwargs.get('cache', True) \
            and (not kwargs.get('reload', False)):
        res = (
            pd.read_parquet(data_file)
            .pipe(pipeline.add_ticker, ticker=ticker)
            .loc[ss_rng[0]:ss_rng[1]]
        )
        if not res.empty:
            logger.debug(f'Loading Bloomberg intraday data from: {data_file}')
            return res

    t_1 = pd.Timestamp('today').date() - pd.Timedelta('1D')
    whole_day = pd.Timestamp(dt).date() < t_1
    batch = kwargs.pop('batch', False)
    if (not whole_day) and batch:
        logger.warning(f'Querying date {t_1} is too close, ignoring download ...')
        return pd.DataFrame()

    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    info_log = f'{ticker} / {cur_dt} / {typ}'

    q_tckr = ticker
    if ex_info.get('is_fut', False):
        if 'freq' not in ex_info:
            logger.error(f'[freq] missing in info for {info_log} ...')

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
        if batch: return pd.DataFrame()
        logger.info(f'{num_trials} trials with no data {info_log}')
        return pd.DataFrame()

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('IntradayBarRequest')

    while conn.bbg_session(**kwargs).tryNextEvent(): pass
    request.set('security', ticker)
    request.set('eventType', typ)
    request.set('interval', kwargs.get('interval', 1))

    time_rng = process.time_range(dt=dt, ticker=ticker, session='allday', **kwargs)
    request.set('startDateTime', time_rng[0])
    request.set('endDateTime', time_rng[1])

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


def bdtick(ticker, dt, session='allday', types=None, **kwargs) -> pd.DataFrame:
    """
    Bloomberg tick data

    Args:
        ticker: ticker name
        dt: date to download
        session: [allday, day, am, pm, pre, post]
        types: str or list, one or combinations of [
            TRADE, AT_TRADE, BID, ASK, MID_PRICE,
            BID_BEST, ASK_BEST, BEST_BID, BEST_ASK,
        ]

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(bdtick, **kwargs)

    exch = const.exch_info(ticker=ticker, **kwargs)
    time_rng = process.time_range(
        dt=dt, ticker=ticker, session=session, tz=exch.tz, **kwargs
    )

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('IntradayTickRequest')

    while conn.bbg_session(**kwargs).tryNextEvent(): pass
    if types is None: types = ['TRADE']
    if isinstance(types, str): types = [types]
    request.set('security', ticker)
    for typ in types: request.append('eventTypes', typ)
    request.set('startDateTime', time_rng[0])
    request.set('endDateTime', time_rng[1])
    request.set('includeConditionCodes', True)
    request.set('includeExchangeCodes', True)
    request.set('includeNonPlottableEvents', True)
    request.set('includeBrokerCodes', True)
    request.set('includeRpsCodes', True)
    request.set('includeTradeTime', True)
    request.set('includeActionCodes', True)
    request.set('includeIndicatorCodes', True)

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


def earning(
        ticker, by='Geo', typ='Revenue', ccy=None, level=None, **kwargs
) -> pd.DataFrame:
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
        pct = f'{yr}_pct'
        data.loc[:, pct] = np.nan

        # Calculate level 1 percentage
        data.loc[data.level == 1, pct] = \
            100 * data.loc[data.level == 1, yr] / data.loc[data.level == 1, yr].sum()

        # Calculate level 2 percentage (higher levels will be ignored)
        sub_pct = []
        for r, snap in data.reset_index()[::-1].iterrows():
            if snap.level > 2: continue
            if snap.level == 1:
                if len(sub_pct) == 0: continue
                data.iloc[sub_pct, data.columns.get_loc(pct)] = \
                    100 * data[yr].iloc[sub_pct] / data[yr].iloc[sub_pct].sum()
                sub_pct = []
            if snap.level == 2: sub_pct.append(r)

    return data


def dividend(
        tickers, typ='all', start_date=None, end_date=None, **kwargs
) -> pd.DataFrame:
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

    fld = {
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
    }.get(typ, typ)

    if (fld == 'Eqy_DVD_Adjust_Fact') and ('Corporate_Actions_Filter' not in kwargs):
        kwargs['Corporate_Actions_Filter'] = 'NORMAL_CASH|ABNORMAL_CASH|CAPITAL_CHANGE'

    if fld in [
        'DVD_Hist_All', 'DVD_Hist', 'Eqy_DVD_Hist_Gross',
        'DVD_Hist_All_with_Amt_Status', 'DVD_Hist_with_Amt_Status',
    ]:
        if start_date:
            kwargs['DVD_Start_Dt'] = utils.fmt_dt(start_date, fmt='%Y%m%d')
        if end_date:
            kwargs['DVD_End_Dt'] = utils.fmt_dt(end_date, fmt='%Y%m%d')

    return bds(tickers=tickers, flds=fld, col_maps={
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
    }, **kwargs)


def beqs(
        screen, asof=None, typ='PRIVATE', group='General', **kwargs
) -> pd.DataFrame:
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

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('BeqsRequest')

    request.set('screenName', screen)
    request.set('screenType', 'GLOBAL' if typ[0].upper() in ['G', 'B'] else 'PRIVATE')
    request.set('Group', group)

    if asof:
        overrides = request.getElement('overrides')
        ovrd = overrides.appendElement()
        ovrd.setElement('fieldId', 'PiTDate')
        ovrd.setElement('value', utils.fmt_dt(asof, '%Y%m%d'))

    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)
    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if res.empty:
        if kwargs.get('trial', 0): return pd.DataFrame()
        else: return beqs(
            screen=screen, asof=asof, typ=typ, group=group, trial=1, **kwargs
        )

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
def subscribe(tickers, flds=None, identity=None, **kwargs):
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
        sub_list.add(topic, flds, correlationId=cid)

    try:
        conn.bbg_session(**kwargs).subscribe(sub_list, identity)
        yield
    finally:
        conn.bbg_session(**kwargs).unsubscribe(sub_list)


async def live(
        tickers, flds='Last_Price', info=None, max_cnt=None, interval=500, **kwargs
):
    """
    Subscribe and getting data feeds from

    Args:
        tickers: list of tickers
        flds: fields to subscribe
        info: list of keys of interests (ticker will be included)
        max_cnt: max number of data points to receive
        interval: update interval, default 500ms

    Yields:
        dict: Bloomberg market data

    Examples:
        >>> info_ = [
        ...     'last_price', 'last_trade', 'bid', 'ask',
        ...     'high', 'low',
        ...     'volume', 'turnover_today_realtime',
        ...     'time',
        ... ]
        >>> async for _ in live('SPY US Equity', max_cnt=10, info=info_):
        ...     pass
    """
    from collections.abc import Iterable

    logger = logs.get_logger(live, **kwargs)
    conv = [conn.blpapi.name.Name]

    def get_value(element: conn.blpapi.Element):
        """
        Get value from element

        Args:
            element: Bloomberg element

        Returns:
            value
        """
        if element.isNull(): return None
        value = element.getValue()
        if isinstance(value, np.bool_): return bool(value)
        if isinstance(value, tuple(conv)): return str(value)
        return value

    if isinstance(flds, str): flds = [flds]
    s_flds = [fld.upper() for fld in flds]
    if isinstance(info, str): info = [info]
    if isinstance(info, Iterable): info = [key.upper() for key in info]

    sess = conn.bbg_session(**kwargs)
    while sess.tryNextEvent(): pass
    with subscribe(tickers=tickers, flds=s_flds, **kwargs):
        cnt = 0
        while True if max_cnt is None else cnt < max_cnt:
            try:
                ev = sess.nextEvent(interval)
                if conn.event_types()[ev.eventType()] != 'SUBSCRIPTION_DATA':
                    continue
                for msg in ev:
                    for fld in s_flds:
                        if not msg.hasElement(fld): continue
                        if msg.getElement(fld).isNull(): continue
                        yield {
                            **{'TICKER': msg.correlationIds()[0].value()},
                            **{
                                str(elem.name()): get_value(elem)
                                for elem in msg.asElement().elements()
                                if (True if not info else str(elem.name()) in info)
                            },
                        }
                        cnt += 1
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
    fut_2 = fut_ticker(gen_ticker=f2, dt=dt, freq=info['freq'], **kwargs)
    fut_1 = fut_ticker(gen_ticker=f1, dt=dt, freq=info['freq'], **kwargs)

    fut_tk = bdp(tickers=[fut_1, fut_2], flds='Last_Tradeable_Dt')

    if pd.Timestamp(dt).month < pd.Timestamp(fut_tk.last_tradeable_dt[0]).month: return fut_1

    dts = pd.bdate_range(end=dt, periods=10)
    volume = bdh(
        fut_tk.index, flds='volume', start_date=dts[0], end_date=dts[-1], keep_one=True
    )
    if volume.empty: return fut_1
    return volume.iloc[-1].idxmax()


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
