import pandas as pd

import time
import pytz
import inspect
import sys
import datetime

from typing import Union
from pathlib import Path

DEFAULT_TZ = pytz.FixedOffset(-time.timezone / 60)


def flatten(iterable, maps=None, unique=False) -> list:
    """
    Flatten any array of items to list

    Args:
        iterable: any array or value
        maps: map items to values
        unique: drop duplicates

    Returns:
        list: flattened list

    References:
        https://stackoverflow.com/a/40857703/1332656

    Examples:
        >>> flatten('abc')
        ['abc']
        >>> flatten(1)
        [1]
        >>> flatten(1.)
        [1.0]
        >>> flatten(['ab', 'cd', ['xy', 'zz']])
        ['ab', 'cd', 'xy', 'zz']
        >>> flatten(['ab', ['xy', 'zz']], maps={'xy': '0x'})
        ['ab', '0x', 'zz']
    """
    if iterable is None: return []
    if maps is None: maps = dict()

    if isinstance(iterable, (str, int, float)):
        return [maps.get(iterable, iterable)]

    x = [maps.get(item, item) for item in _to_gen_(iterable)]
    return list(set(x)) if unique else x


def _to_gen_(iterable):
    """
    Recursively iterate lists and tuples
    """
    from collections.abc import Iterable

    for elm in iterable:
        if isinstance(elm, Iterable) and not isinstance(elm, (str, bytes)):
            yield from _to_gen_(elm)
        else: yield elm


def fmt_dt(dt, fmt='%Y-%m-%d') -> str:
    """
    Format date string

    Args:
        dt: any date format
        fmt: output date format

    Returns:
        str: date format

    Examples:
        >>> fmt_dt(dt='2018-12')
        '2018-12-01'
        >>> fmt_dt(dt='2018-12-31', fmt='%Y%m%d')
        '20181231'
    """
    return pd.Timestamp(dt).strftime(fmt)


def cur_time(typ='date', tz=DEFAULT_TZ) -> Union[datetime.date, str]:
    """
    Current time

    Args:
        typ: one of ['date', 'time', 'time_path', 'raw', '']
        tz: timezone

    Returns:
        relevant current time or date

    Examples:
        >>> cur_dt = pd.Timestamp('now')
        >>> cur_time(typ='date') == cur_dt.strftime('%Y-%m-%d')
        True
        >>> cur_time(typ='time') == cur_dt.strftime('%Y-%m-%d %H:%M:%S')
        True
        >>> cur_time(typ='time_path') == cur_dt.strftime('%Y-%m-%d/%H-%M-%S')
        True
        >>> isinstance(cur_time(typ='raw', tz='Europe/London'), pd.Timestamp)
        True
        >>> cur_time(typ='') == cur_dt.date()
        True
    """
    dt = pd.Timestamp('now', tz=tz)

    if typ == 'date': return dt.strftime('%Y-%m-%d')
    if typ == 'time': return dt.strftime('%Y-%m-%d %H:%M:%S')
    if typ == 'time_path': return dt.strftime('%Y-%m-%d/%H-%M-%S')
    if typ == 'raw': return dt

    return dt.date()


class FString(object):

    def __init__(self, str_fmt):
        self.str_fmt = str_fmt

    def __str__(self):
        kwargs = inspect.currentframe().f_back.f_globals.copy()
        kwargs.update(inspect.currentframe().f_back.f_locals)
        return self.str_fmt.format(**kwargs)


def fstr(fmt, **kwargs) -> str:
    """
    Delayed evaluation of f-strings

    Args:
        fmt: f-string but in terms of normal string, i.e., '{path}/{file}.parq'
        **kwargs: variables for f-strings, i.e., path, file = '/data', 'daily'

    Returns:
        FString object

    References:
        https://stackoverflow.com/a/42497694/1332656
        https://stackoverflow.com/a/4014070/1332656

    Examples:
        >>> fmt = '{data_path}/{data_file}.parq'
        >>> fstr(fmt, data_path='your/data/path', data_file='sample')
        'your/data/path/sample.parq'
    """
    locals().update(kwargs)
    return f'{FString(str_fmt=fmt)}'


def to_str(
        data: dict, fmt='{key}={value}', sep=', ', public_only=True
) -> str:
    """
    Convert dict to string

    Args:
        data: dict
        fmt: how key and value being represented
        sep: how pairs of key and value are seperated
        public_only: if display public members only

    Returns:
        str: string representation of dict

    Examples:
        >>> test_dict = dict(b=1, a=0, c=2, _d=3)
        >>> to_str(test_dict)
        '{b=1, a=0, c=2}'
        >>> to_str(test_dict, sep='|')
        '{b=1|a=0|c=2}'
        >>> to_str(test_dict, public_only=False)
        '{b=1, a=0, c=2, _d=3}'
    """
    if public_only: keys = list(filter(lambda vv: vv[0] != '_', data.keys()))
    else: keys = list(data.keys())
    return '{' + sep.join([
        to_str(data=v, fmt=fmt, sep=sep)
        if isinstance(v, dict) else fstr(fmt=fmt, key=k, value=v)
        for k, v in data.items() if k in keys
    ]) + '}'


def func_scope(func) -> str:
    """
    Function scope name

    Args:
        func: python function

    Returns:
        str: module_name.func_name

    Examples:
        >>> func_scope(flatten)
        'xbbg.core.utils.flatten'
        >>> func_scope(time.strftime)
        'time.strftime'
    """
    cur_mod = sys.modules[func.__module__]
    return f'{cur_mod.__name__}.{func.__name__}'


def load_module(full_path):
    """
    Load module from full path
    Args:
        full_path: module full path name
    Returns:
        python module
    References:
        https://stackoverflow.com/a/67692/1332656
    Examples:
        >>> from pathlib import Path
        >>>
        >>> cur_path = Path(__file__).parent
        >>> load_module(cur_path / 'timezone.py').__name__
        'timezone'
        >>> load_module(cur_path / 'timezone.pyc')
        Traceback (most recent call last):
        ImportError: not a python file: timezone.pyc
    """
    from importlib import util

    file_name = Path(full_path).name
    if file_name[-3:] != '.py':
        raise ImportError(f'not a python file: {file_name}')
    module_name = file_name[:-3]

    spec = util.spec_from_file_location(name=module_name, location=full_path)
    module = util.module_from_spec(spec=spec)
    spec.loader.exec_module(module=module)

    return module
