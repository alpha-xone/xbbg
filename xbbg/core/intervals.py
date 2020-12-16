import pandas as pd
import numpy as np

from collections import namedtuple

from xbbg import const
from xbbg.io import logs, param

Session = namedtuple('Session', ['start_time', 'end_time'])
SessNA = Session(None, None)


def get_interval(ticker, session, **kwargs) -> Session:
    """
    Get interval from defined session

    Args:
        ticker: ticker
        session: session

    Returns:
        Session of start_time and end_time

    Examples:
        >>> get_interval('005490 KS Equity', 'day_open_30')
        Session(start_time='09:00', end_time='09:30')
        >>> get_interval('005490 KS Equity', 'day_normal_30_20')
        Session(start_time='09:31', end_time='15:00')
        >>> get_interval('005490 KS Equity', 'day_close_20')
        Session(start_time='15:01', end_time='15:20')
        >>> get_interval('700 HK Equity', 'am_open_30')
        Session(start_time='09:30', end_time='10:00')
        >>> get_interval('700 HK Equity', 'am_normal_30_30')
        Session(start_time='10:01', end_time='11:30')
        >>> get_interval('700 HK Equity', 'am_close_30')
        Session(start_time='11:31', end_time='12:00')
        >>> get_interval('ES1 Index', 'day_exact_2130_2230')
        Session(start_time=None, end_time=None)
        >>> get_interval('ES1 Index', 'allday_exact_2130_2230')
        Session(start_time='21:30', end_time='22:30')
        >>> get_interval('ES1 Index', 'allday_exact_2130_0230')
        Session(start_time='21:30', end_time='02:30')
        >>> get_interval('AMLP US', 'day_open_30')
        Session(start_time=None, end_time=None)
        >>> get_interval('7974 JP Equity', 'day_normal_180_300') is SessNA
        True
        >>> get_interval('Z 1 Index', 'allday_normal_30_30')
        Session(start_time='01:31', end_time='20:30')
        >>> get_interval('GBP Curncy', 'day')
        Session(start_time='17:02', end_time='17:00')
    """
    if '_' not in session:
        session = f'{session}_normal_0_0'
    interval = Intervals(ticker=ticker, **kwargs)
    ss_info = session.split('_')
    return getattr(interval, f'market_{ss_info.pop(1)}')(*ss_info)


def shift_time(start_time, mins) -> str:
    """
    Shift start time by mins

    Args:
        start_time: start time in terms of HH:MM string
        mins: number of minutes (+ / -)

    Returns:
        end time in terms of HH:MM string
    """
    s_time = pd.Timestamp(start_time)
    e_time = s_time + np.sign(mins) * pd.Timedelta(f'00:{abs(mins)}:00')
    return e_time.strftime('%H:%M')


class Intervals(object):

    def __init__(self, ticker, **kwargs):
        """
        Args:
            ticker: ticker
        """
        self.ticker = ticker
        self.exch = const.exch_info(ticker=ticker, **kwargs)

    def market_open(self, session, mins) -> Session:
        """
        Time intervals for market open

        Args:
            session: [allday, day, am, pm, night]
            mins: mintues after open

        Returns:
            Session of start_time and end_time
        """
        if session not in self.exch: return SessNA
        start_time = self.exch[session][0]
        return Session(start_time, shift_time(start_time, int(mins)))

    def market_close(self, session, mins) -> Session:
        """
        Time intervals for market close

        Args:
            session: [allday, day, am, pm, night]
            mins: mintues before close

        Returns:
            Session of start_time and end_time
        """
        if session not in self.exch: return SessNA
        end_time = self.exch[session][-1]
        return Session(shift_time(end_time, -int(mins) + 1), end_time)

    def market_normal(self, session, after_open, before_close) -> Session:
        """
        Time intervals between market

        Args:
            session: [allday, day, am, pm, night]
            after_open: mins after open
            before_close: mins before close

        Returns:
            Session of start_time and end_time
        """
        logger = logs.get_logger(self.market_normal)

        if session not in self.exch: return SessNA
        ss = self.exch[session]

        s_time = shift_time(ss[0], int(after_open) + 1)
        e_time = shift_time(ss[-1], -int(before_close))

        request_cross = pd.Timestamp(s_time) >= pd.Timestamp(e_time)
        session_cross = pd.Timestamp(ss[0]) >= pd.Timestamp(ss[1])
        if request_cross and (not session_cross):
            logger.warning(f'end time {e_time} is earlier than {s_time} ...')
            return SessNA

        return Session(s_time, e_time)

    def market_exact(self, session, start_time: str, end_time: str) -> Session:
        """
        Explicitly specify start time and end time

        Args:
            session: predefined session
            start_time: start time in terms of HHMM string
            end_time: end time in terms of HHMM string

        Returns:
            Session of start_time and end_time
        """
        if session not in self.exch: return SessNA
        ss = self.exch[session]

        same_day = ss[0] < ss[-1]

        if not start_time: s_time = ss[0]
        else:
            s_time = param.to_hours(int(start_time))
            if same_day: s_time = max(s_time, ss[0])

        if not end_time: e_time = ss[-1]
        else:
            e_time = param.to_hours(int(end_time))
            if same_day: e_time = min(e_time, ss[-1])

        if same_day and (s_time > e_time): return SessNA
        return Session(start_time=s_time, end_time=e_time)
