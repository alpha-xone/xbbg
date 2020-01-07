import pandas as pd
import numpy as np

import blpapi
import datetime

from contextlib import contextmanager

from xbbg import const, pipeline
from xbbg.io import logs, files, storage
from xbbg.core import utils, conn, process


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

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('ReferenceDataRequest')

    process.init_request(request=request, tickers=tickers, flds=flds, **kwargs)
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.bbg_session(**kwargs).sendRequest(request)

    res = dict()
    for r in process.receive_events(func=process.process_ref): res.update(r)
    if not res: return pd.DataFrame()

    col_maps = kwargs.get('col_maps', None)
    return (
        pd.DataFrame(res)
        .transpose()
        .reindex(columns=flds)
        .dropna(how='all', axis=1)
        .pipe(pipeline.format_raw)
        .pipe(pipeline.standard_cols, col_maps=col_maps)
    )


def bds(tickers, flds, **kwargs) -> pd.DataFrame:
    """
    Bloomberg block data

    Args:
        tickers: ticker(s)
        flds: field
        **kwargs: other overrides for query

    Returns:
        pd.DataFrame: block data
    """
    logger = logs.get_logger(bds, **kwargs)

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('ReferenceDataRequest')

    if isinstance(tickers, str):
        data_file = storage.ref_file(
            ticker=tickers, fld=flds, has_date=True, ext='parq', **kwargs
        )
        if files.exists(data_file):
            logger.debug(f'Loading Bloomberg data from: {data_file}')
            return pd.DataFrame(pd.read_parquet(data_file))

        process.init_request(request=request, tickers=tickers, flds=flds, **kwargs)
        logger.debug(f'Sending request to Bloomberg ...\n{request}')
        conn.bbg_session(**kwargs).sendRequest(request)

        res = dict()
        for r in process.receive_events(func=process.process_ref): res.update(r)
        final = res.get(tickers, {}).get(flds, {})
        col_maps = kwargs.get('col_maps', None)
        if not final: return pd.DataFrame()
        data = (
            pd.DataFrame(final)
            .rename(index=lambda _: tickers)
            .pipe(pipeline.format_raw)
            .pipe(pipeline.standard_cols, col_maps=col_maps)
        )
        if data_file:
            logger.debug(f'Saving Bloomberg data to: {data_file}')
            files.create_folder(data_file, is_file=True)
            data.to_parquet(data_file)
        return data
    else:
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
    conn.bbg_session(**kwargs).sendRequest(request)

    res = pd.DataFrame(process.receive_events(process.process_hist))
    if res.empty: return pd.DataFrame()
    return pd.DataFrame(pd.concat([
        r.apply(pd.Series) for _, r in res.iterrows()
    ], axis=1))


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
            batch: whether is batch process to download data
            log: level of logs

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(bdib, **kwargs)

    ss_rng = process.time_range(dt=dt, ticker=ticker, session=session)
    data_file = storage.bar_file(ticker=ticker, dt=dt, typ=typ)
    if files.exists(data_file):
        return pd.read_parquet(data_file).loc[ss_rng[0]:ss_rng[1]]

    exch = const.exch_info(ticker=ticker)
    if exch.empty: raise KeyError(f'Cannot find exchange info for {ticker}')

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('IntradayBarRequest')

    request.set('security', ticker)
    request.set('eventType', typ)
    request.set('interval', kwargs.get('interval', 1))

    time_rng = process.time_range(dt=dt, ticker=ticker, session='allday')
    request.set('startDateTime', time_rng[0])
    request.set('endDateTime', time_rng[1])

    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.bbg_session(**kwargs).sendRequest(request)

    res = pd.DataFrame(process.receive_events(func=process.process_bar, ticker=ticker))
    if res.empty: return pd.DataFrame()
    data = (
        pd.DataFrame(pd.concat(res.iloc[0].values, axis=1))
        .transpose()
        .tz_localize('UTC')
        .tz_convert(exch.tz)
    )
    storage.save_intraday(data=data, ticker=ticker, dt=dt, typ=typ)
    return data.loc[ss_rng[0]:ss_rng[1]]


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
    ovrd = 'G' if by[0].upper() == 'G' else 'P'
    new_kw = dict(raw=True, Product_Geo_Override=ovrd)
    header = bds(tickers=ticker, flds='PG_Bulk_Header', **new_kw, **kwargs)
    if ccy: kwargs['Eqy_Fund_Crncy'] = ccy
    if level: kwargs['PG_Hierarchy_Level'] = level
    data = bds(tickers=ticker, flds=f'PG_{typ}', **new_kw, **kwargs)

    if data.empty or header.empty: return pd.DataFrame()
    if data.shape[1] != header.shape[1]:
        raise ValueError(f'Inconsistent shape of data and header')
    data.columns = (
        header.iloc[0]
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('_20', '20')
        .tolist()
    )

    if 'level' not in data: raise KeyError(f'Cannot find [level] in data')
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
        typ: GLOBAL (Bloomberg) or PRIVATE (Custom, default)
        group: group name if screen is organized into groups

    Returns:
        pd.DataFrame
    """
    logger = logs.get_logger(beqs, **kwargs)

    service = conn.bbg_service(service='//blp/refdata', **kwargs)
    request = service.createRequest('BeqsRequest')

    request.set('screenName', screen)
    request.set('screenType', typ)
    request.set('Group', group)

    if asof:
        overrides = request.getElement('overrides')
        ovrd = overrides.appendElement()
        ovrd.setElement('fieldId', 'PiTDate')
        ovrd.setElement('value', utils.fmt_dt(asof, '%Y%m%d'))

    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.bbg_session(**kwargs).sendRequest(request)
    res = pd.DataFrame(process.receive_events(func=process.process_ref))
    if res.empty: return pd.DataFrame()
    return (
        pd.DataFrame(res.iloc[0].tolist())
        .pipe(pipeline.format_raw)
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


def live(
        tickers, flds='Last_Price', max_cnt=None, json=False, **kwargs
) -> dict:
    """
    Subscribe and getting data feeds from

    Args:
        tickers: list of tickers
        flds: fields to subscribe
        max_cnt: max number of data points to receive
        json: if data is required to convert to json

    Yields:
        dict: Bloomberg market data
    """
    logger = logs.get_logger(live, **kwargs)

    def get_value(element):
        """
        Get value from element

        Args:
            element: Bloomberg element

        Returns:
            dict
        """
        conv = [blpapi.name.Name]
        if json: conv += [pd.Timestamp, datetime.time, datetime.date]
        if element.isNull(): return None
        value = element.getValue()
        if isinstance(value, np.bool_): return bool(value)
        if isinstance(value, tuple(conv)): return str(value)
        return value

    if isinstance(flds, str): flds = [flds]
    s_flds = [fld.upper() for fld in flds]
    with subscribe(tickers=tickers, flds=s_flds, **kwargs):
        cnt = 0
        while True if max_cnt is None else cnt < max_cnt:
            try:
                ev = conn.bbg_session(**kwargs).nextEvent(500)
                if conn.event_types()[ev.eventType()] != 'SUBSCRIPTION_DATA':
                    continue
                for msg in ev:
                    for fld in s_flds:
                        if not msg.hasElement(fld): continue
                        if msg.getElement(fld).isNull(): continue
                        ticker = msg.correlationIds()[0].value()
                        values = {**{'TICKER': ticker}, **{
                            str(elem.name()): get_value(elem)
                            for elem in msg.asElement().elements()
                        }}
                        yield {
                            key: value for key, value in values.items()
                            if value not in [np.nan, pd.NaT, None]
                            or (isinstance(value, str) and value.strip())
                        }
                        cnt += 1
            except ValueError as e: logger.debug(e)
            except KeyboardInterrupt: break
