import pandas as pd
import numpy as np

import pytest
try: import blpapi
except ImportError: blpapi = pytest.importorskip('blpapi')

from itertools import starmap
from collections import OrderedDict

from xbbg import const
from xbbg.core.timezone import DEFAULT_TZ
from xbbg.core import intervals, overrides, conn

RESPONSE_ERROR = blpapi.Name("responseError")
SESSION_TERMINATED = blpapi.Name("SessionTerminated")
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")
BAR_DATA = blpapi.Name('barData')
BAR_TICK = blpapi.Name('barTickData')
TICK_DATA = blpapi.Name('tickData')


def create_request(
        service: str,
        request: str,
        settings: list = None,
        ovrds: list = None,
        append: dict = None,
        **kwargs,
) -> blpapi.request.Request:
    """
    Create request for query

    Args:
        service: service name
        request: request name
        settings: list of settings
        ovrds: list of overrides
        append: info to be appended to request directly
        kwargs: other overrides

    Returns:
        Bloomberg request
    """
    srv = conn.bbg_service(service=service, **kwargs)
    req = srv.createRequest(request)

    list(starmap(req.set, settings if settings else []))
    if ovrds:
        ovrd = req.getElement('overrides')
        for fld, val in ovrds:
            item = ovrd.appendElement()
            item.setElement('fieldId', fld)
            item.setElement('value', val)
    if append:
        for key, val in append.items():
            vals = [val] if isinstance(val, str) else val
            for v in vals: req.append(key, v)

    return req


def init_request(request: blpapi.request.Request, tickers, flds, **kwargs):
    """
    Initiate Bloomberg request instance

    Args:
        request: Bloomberg request to initiate and append
        tickers: tickers
        flds: fields
        **kwargs: overrides and
    """
    while conn.bbg_session(**kwargs).tryNextEvent(): pass

    if isinstance(tickers, str): tickers = [tickers]
    for ticker in tickers: request.append('securities', ticker)

    if isinstance(flds, str): flds = [flds]
    for fld in flds: request.append('fields', fld)

    adjust = kwargs.pop('adjust', None)
    if isinstance(adjust, str) and adjust:
        if adjust == 'all':
            kwargs['CshAdjNormal'] = True
            kwargs['CshAdjAbnormal'] = True
            kwargs['CapChg'] = True
        else:
            kwargs['CshAdjNormal'] = 'normal' in adjust or 'dvd' in adjust
            kwargs['CshAdjAbnormal'] = 'abn' in adjust or 'dvd' in adjust
            kwargs['CapChg'] = 'split' in adjust

    if 'start_date' in kwargs: request.set('startDate', kwargs.pop('start_date'))
    if 'end_date' in kwargs: request.set('endDate', kwargs.pop('end_date'))

    for elem_name, elem_val in overrides.proc_elms(**kwargs):
        request.set(elem_name, elem_val)

    ovrds = request.getElement('overrides')
    for ovrd_fld, ovrd_val in overrides.proc_ovrds(**kwargs):
        ovrd = ovrds.appendElement()
        ovrd.setElement('fieldId', ovrd_fld)
        ovrd.setElement('value', ovrd_val)


def time_range(dt, ticker, session='allday', tz='UTC', **kwargs) -> intervals.Session:
    """
    Time range in UTC (for intraday bar) or other timezone

    Args:
        dt: date
        ticker: ticker
        session: market session defined in xbbg/markets/exch.yml
        tz: timezone

    Returns:
        intervals.Session
    """
    ss = intervals.get_interval(ticker=ticker, session=session, **kwargs)
    ex_info = const.exch_info(ticker=ticker, **kwargs)
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    time_fmt = '%Y-%m-%dT%H:%M:%S'
    time_idx = (
        pd.DatetimeIndex([
            f'{cur_dt} {ss.start_time}',
            f'{cur_dt} {ss.end_time}'],
        )
        .tz_localize(ex_info.tz)
        .tz_convert(DEFAULT_TZ)
        .tz_convert(tz)
    )
    if time_idx[0] > time_idx[1]: time_idx -= pd.TimedeltaIndex(['1D', '0D'])
    return intervals.Session(time_idx[0].strftime(time_fmt), time_idx[1].strftime(time_fmt))


def rec_events(func, **kwargs):
    """
    Receive events received from Bloomberg

    Args:
        func: must be generator function
        **kwargs: arguments for input function

    Yields:
        Elements of Bloomberg responses
    """
    timeout_counts = 0
    responses = [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]
    timeout = kwargs.pop('timeout', 500)
    while True:
        ev = conn.bbg_session(**kwargs).nextEvent(timeout=timeout)
        if ev.eventType() in responses:
            for msg in ev:
                for r in func(msg=msg, **kwargs):
                    yield r
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        elif ev.eventType() == blpapi.Event.TIMEOUT:
            timeout_counts += 1
            if timeout_counts > 20:
                break
        else:
            for _ in ev:
                if getattr(ev, 'messageType', lambda: None)() \
                    == SESSION_TERMINATED: break


def process_ref(msg: blpapi.message.Message, **kwargs) -> dict:
    """
    Process reference messages from Bloomberg

    Args:
        msg: Bloomberg reference data messages from events

    Returns:
        dict
    """
    kwargs.pop('(@_<)', None)
    data = None
    if msg.hasElement('securityData'):
        data = msg.getElement('securityData')
    elif msg.hasElement('data') and \
            msg.getElement('data').hasElement('securityData'):
        data = msg.getElement('data').getElement('securityData')
    if not data: return iter([])

    for sec in data.values():
        ticker = sec.getElement('security').getValue()
        for fld in sec.getElement('fieldData').elements():
            info = [('ticker', ticker), ('field', str(fld.name()))]
            if fld.isArray():
                for item in fld.values():
                    yield OrderedDict(info + [
                        (
                            str(elem.name()),
                            None if elem.isNull() else elem.getValue()
                        )
                        for elem in item.elements()
                    ])
            else:
                yield OrderedDict(info + [
                    ('value', None if fld.isNull() else fld.getValue()),
                ])


def process_hist(msg: blpapi.message.Message, **kwargs) -> dict:
    """
    Process historical data messages from Bloomberg

    Args:
        msg: Bloomberg historical data messages from events

    Returns:
        dict
    """
    kwargs.pop('(>_<)', None)
    if not msg.hasElement('securityData'): return {}
    ticker = msg.getElement('securityData').getElement('security').getValue()
    for val in msg.getElement('securityData').getElement('fieldData').values():
        if val.hasElement('date'):
            yield OrderedDict([('ticker', ticker)] + [
                (str(elem.name()), elem.getValue()) for elem in val.elements()
            ])


def process_bar(msg: blpapi.message.Message, typ='bar', **kwargs) -> OrderedDict:
    """
    Process Bloomberg intraday bar messages

    Args:
        msg: Bloomberg intraday bar messages from events
        typ: `bar` or `tick`

    Yields:
        OrderedDict
    """
    kwargs.pop('(#_#)', None)
    check_error(msg=msg)
    if typ[0].lower() == 't':
        lvls = [TICK_DATA, TICK_DATA]
    else:
        lvls = [BAR_DATA, BAR_TICK]

    if msg.hasElement(lvls[0]):
        for bar in msg.getElement(lvls[0]).getElement(lvls[1]).values():
            yield OrderedDict([
                (str(elem.name()), elem.getValue())
                for elem in bar.elements()
            ])


def check_error(msg):
    """
    Check error in message
    """
    if msg.hasElement(RESPONSE_ERROR):
        error = msg.getElement(RESPONSE_ERROR)
        raise ValueError(
            f'[Intraday Bar Error] '
            f'{error.getElementAsString(CATEGORY)}: '
            f'{error.getElementAsString(MESSAGE)}'
        )


def elem_value(element: conn.blpapi.Element):
    """
    Get value from element

    Args:
        element: Bloomberg element

    Returns:
        value
    """
    if element.isNull(): return None
    try: value = element.getValue()
    except ValueError: return None
    if isinstance(value, np.bool_): return bool(value)
    if isinstance(value, conn.blpapi.name.Name): return str(value)
    return value


def earning_pct(data: pd.DataFrame, yr):
    """
    Calculate % of earnings by year
    """
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


def check_current(dt, logger, **kwargs) -> bool:
    """
    Check current time against T-1
    """
    t_1 = pd.Timestamp('today').date() - pd.Timedelta('1D')
    whole_day = pd.Timestamp(dt).date() < t_1
    if (not whole_day) and kwargs.get('batch', False):
        logger.warning(f'Querying date {t_1} is too close, ignoring download ...')
        return False
    return True
