import pandas as pd
import numpy as np

from xone import logs

from xbbg.core import const
from xbbg.exchange import Session, SessNA

ValidSessions = ['allday', 'day', 'am', 'pm', 'night']


def get_interval(ticker, session):
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
    """
    interval = Intervals(ticker=ticker)
    ss_info = session.split('_')
    return getattr(interval, f'market_{ss_info.pop(1)}')(*ss_info)


def shift_time(start_time, mins):
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

    def __init__(self, ticker):
        """
        Args:
            ticker: ticker
        """
        self.logger = logs.get_logger(Intervals)
        self.ticker = ticker
        self.exch = const.market_info(ticker=self.ticker).get('exch', None)
        self.active_ss = dict()

        if self.exch is None:
            self.logger.error(f'cannot find exch info for {ticker} ...')
            self.mkt_ss = None

        else:
            self.mkt_ss = self.exch.hours
            for fld in getattr(self.mkt_ss, '_fields'):
                if fld not in ValidSessions: continue
                ss = getattr(self.mkt_ss, fld)
                if ss == SessNA: continue
                self.active_ss[fld] = ss

    def market_open(self, session, mins):
        """
        Time intervals for market open

        Args:
            session: [allday, day, am, pm, night]
            mins: mintues after open

        Returns:
            Session of start_time and end_time
        """
        if session not in self.active_ss: return SessNA
        ss = getattr(self.mkt_ss, session)

        return Session(ss.start_time, shift_time(ss.start_time, int(mins)))

    def market_close(self, session, mins):
        """
        Time intervals for market close

        Args:
            session: [allday, day, am, pm, night]
            mins: mintues before close

        Returns:
            Session of start_time and end_time
        """
        if session not in self.active_ss: return SessNA
        ss = self.active_ss[session]

        return Session(shift_time(ss.end_time, -int(mins) + 1), ss.end_time)

    def market_normal(self, session, after_open, before_close):
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

        if session not in self.active_ss: return SessNA
        ss = self.active_ss[session]

        s_time = shift_time(ss.start_time, int(after_open) + 1)
        e_time = shift_time(ss.end_time, -int(before_close))

        if pd.Timestamp(s_time) >= pd.Timestamp(e_time):
            logger.warning(f'end time {e_time} is earlier than {s_time} ...')
            return SessNA

        return Session(s_time, e_time)

    def market_exact(self, session, start_time, end_time):
        """
        Explicitly specify start time and end time

        Args:
            session: predefined session
            start_time: start time in terms of HHMM string
            end_time: end time in terms of HHMM string

        Returns:
            Session of start_time and end_time
        """
        if session not in self.active_ss: return SessNA
        ss = self.active_ss[session]

        same_day = ss.start_time < ss.end_time

        if start_time == '': s_time = ss.start_time
        else:
            s_time = f'{start_time[:2]}:{start_time[-2:]}'
            if same_day: s_time = max(s_time, ss.start_time)

        if end_time == '': e_time = ss.end_time
        else:
            e_time = f'{end_time[:2]}:{end_time[-2:]}'
            if same_day: e_time = min(e_time, ss.end_time)

        if same_day and (s_time > e_time): return SessNA
        return Session(start_time=s_time, end_time=e_time)
