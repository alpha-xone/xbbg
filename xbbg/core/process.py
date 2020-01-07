import pandas as pd

import blpapi

from xbbg import const
from xbbg.core.timezone import DEFAULT_TZ
from xbbg.core import intervals, assist, conn, names


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

    elems = assist.proc_elms(**kwargs)
    for elem_name, elem_val in elems: request.set(elem_name, elem_val)

    ovrds = assist.proc_ovrds(**kwargs)
    overrides = request.getElement('overrides')
    for ovrd_fld, ovrd_val in ovrds:
        ovrd = overrides.appendElement()
        ovrd.setElement('fieldId', ovrd_fld)
        ovrd.setElement('value', ovrd_val)


def time_range(dt, ticker, session='allday', tz='UTC') -> intervals.Session:
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
    ss = intervals.get_interval(ticker=ticker, session=session)
    exch = const.exch_info(ticker=ticker)
    cur_dt = pd.Timestamp(dt).strftime('%Y-%m-%d')
    time_fmt = '%Y-%m-%dT%H:%M:%S'
    time_idx = pd.DatetimeIndex([
        f'{cur_dt} {ss.start_time}', f'{cur_dt} {ss.end_time}']
    ).tz_localize(exch.tz).tz_convert(DEFAULT_TZ).tz_convert(tz)
    if time_idx[0] > time_idx[1]: time_idx -= pd.TimedeltaIndex(['1D', '0D'])
    return intervals.Session(time_idx[0].strftime(time_fmt), time_idx[1].strftime(time_fmt))


def receive_events(func, **kwargs):
    """
    Receive events received from Bloomberg
    """
    timeout_counts = 0
    responses = [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]
    while True:
        ev = conn.bbg_session(**kwargs).nextEvent(500)
        if ev.eventType() in responses:
            for msg in ev:
                yield func(msg=msg, **kwargs)
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        elif ev.eventType() == blpapi.Event.TIMEOUT:
            timeout_counts += 1
            if timeout_counts > 10:
                break
        else:
            for _ in ev:
                if getattr(ev, 'messageType', lambda: None)() \
                    == names.SESSION_TERMINATED: break


def process_ref(msg: blpapi.message.Message) -> dict:
    """
    Process reference messages from Bloomberg

    Args:
        msg: Bloomberg reference data messages from events

    Returns:
        dict
    """
    data = None
    if msg.hasElement('securityData'):
        data = msg.getElement('securityData')
    elif msg.hasElement('data') and \
            msg.getElement('data').hasElement('securityData'):
        data = msg.getElement('data').getElement('securityData')
    if data is None: return {}

    return {
        sec.getElement('security').getValue(): {
            str(fld.name()): [
                {
                    str(elem.name()): None if elem.isNull() else elem.getValue()
                    for elem in item.elements()
                }
                for item in fld.values()
            ] if fld.isArray() else (
                None if fld.isNull() else fld.getValue()
            )
            for fld in sec.getElement('fieldData').elements()
        }
        for sec in data.values()
    }


def process_hist(msg: blpapi.message.Message) -> dict:
    """
    Process historical data messages from Bloomberg

    Args:
        msg: Bloomberg historical data messages from events

    Returns:
        dict
    """
    if not msg.hasElement('securityData'): return {}
    return {
        elem_dt.getElement('date').getValue(): {
            (
                msg.getElement('securityData').getElement('security').getValue(),
                str(elem.name())
            ): elem.getValue()
            for elem in elem_dt.elements() if str(elem.name()) != 'date'
        }
        for elem_dt in msg.getElement('securityData').getElement('fieldData').values()
        if elem_dt.hasElement('date')
    }


def process_bar(msg: blpapi.message.Message, ticker: str) -> pd.Series:
    """
    Process Bloomberg intraday bar messages

    Args:
        msg: Bloomberg intraday bar messages from events
        ticker: ticker

    Yields:
        pd.Series
    """
    check_error(msg=msg)
    data_rows = msg.getElement(names.BAR_DATA).getElement(names.TICK_DATA)
    for bar in data_rows.values():
        yield pd.Series({
            (ticker, fld):
            getattr(bar, res[0])(res[1])
            for fld, res in names.BAR_ITEMS.items()
        }, name=getattr(bar, names.BAR_TITLE[0])(names.BAR_TITLE[1]))


def check_error(msg):
    """
    Check error in message
    """
    if msg.hasElement(names.RESPONSE_ERROR):
        error = msg.getElement(names.RESPONSE_ERROR)
        raise ValueError(
            f'[Intraday Bar Error] '
            f'{error.getElementAsString(names.CATEGORY)}: '
            f'{error.getElementAsString(names.MESSAGE)}'
        )
