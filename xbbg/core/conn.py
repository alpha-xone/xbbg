import pandas as pd

import inspect
import pytest

from functools import wraps

from xbbg.core import utils, assist
from xbbg.io import files, logs, storage, cached

try:
    import pdblp
except ImportError:
    pdblp = utils.load_module(f'{files.abspath(__file__)}/pdblp.py')

_CON_SYM_ = '_xcon_'
_PORT_, _TIMEOUT_ = 8194, 30000

if hasattr(pytest, 'config'):
    if not pytest.config.option.with_bbg:
        pytest.skip('no Bloomberg')


def with_bloomberg(func):
    """
    Wrapper function for Bloomberg connection

    Args:
        func: function to wrap
    """
    @wraps(func)
    def wrapper(*args, **kwargs):

        param = inspect.signature(func).parameters
        port = kwargs.pop('port', _PORT_)
        timeout = kwargs.pop('timeout', _TIMEOUT_)
        restart = kwargs.pop('restart', False)
        all_kw = {
            k: args[n] if n < len(args) else v.default
            for n, (k, v) in enumerate(param.items()) if k != 'kwargs'
        }
        all_kw.update(kwargs)
        log_level = kwargs.get('log', logs.LOG_LEVEL)

        for to_list in ['tickers', 'flds']:
            conv = all_kw.get(to_list, None)
            if hasattr(conv, 'tolist'):
                all_kw[to_list] = getattr(conv, 'tolist')()
            if isinstance(conv, str):
                all_kw[to_list] = [conv]

        cached_data = []
        if func.__name__ in ['bdp', 'bds']:
            to_qry = cached.bdp_bds_cache(func=func.__name__, **all_kw)
            cached_data += to_qry.cached_data

            if not (to_qry.tickers and to_qry.flds):
                if not cached_data: return pd.DataFrame()
                res = pd.concat(cached_data, sort=False).reset_index(drop=True)
                if not all_kw.get('raw', False):
                    res = assist.format_output(
                        data=res, source=func.__name__,
                        col_maps=all_kw.get('col_maps', dict())
                    )
                return res

            all_kw['tickers'] = to_qry.tickers
            all_kw['flds'] = to_qry.flds

        if func.__name__ in ['bdib']:
            data_file = storage.hist_file(
                ticker=all_kw['ticker'], dt=all_kw['dt'], typ=all_kw['typ'],
            )
            if files.exists(data_file):
                logger = logs.get_logger(func, level=log_level)
                if all_kw.get('batch', False): return
                logger.debug(f'reading from {data_file} ...')
                return assist.format_intraday(
                    data=pd.read_parquet(data_file), ticker=all_kw['ticker']
                )

        raw = all_kw.pop('raw', False)
        col_maps = all_kw.pop('col_maps', dict())
        _, new = create_connection(port=port, timeout=timeout, restart=restart)
        res = func(**all_kw)
        if new: delete_connection()

        if isinstance(res, list):
            final = cached_data + res
            if not final: return pd.DataFrame()
            res = pd.DataFrame(pd.concat(final, sort=False))

        if (func.__name__ in ['bdp', 'bds']) and (not raw):
            res = assist.format_output(
                data=res.reset_index(drop=True),
                source=func.__name__, col_maps=col_maps,
            )

        return res
    return wrapper


def create_connection(
        port=_PORT_, timeout=_TIMEOUT_, restart=False
):
    """
    Create Bloomberg connection

    Returns:
        (Bloomberg connection, if connection is new)
    """
    if _CON_SYM_ in globals():
        if not isinstance(globals()[_CON_SYM_], pdblp.BCon):
            del globals()[_CON_SYM_]

    if (_CON_SYM_ in globals()) and (not restart):
        con = globals()[_CON_SYM_]
        if getattr(con, '_session').start(): con.start()
        return con, False

    else:
        con = pdblp.BCon(port=port, timeout=timeout)
        globals()[_CON_SYM_] = con
        con.start()
        return con, True


def delete_connection():
    """
    Stop and destroy Bloomberg connection
    """
    if _CON_SYM_ in globals():
        con = globals().pop(_CON_SYM_)
        if not getattr(con, '_session').start(): con.stop()
