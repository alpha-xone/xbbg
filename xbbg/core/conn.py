import pandas as pd

import inspect
import pytest

from functools import wraps
from itertools import product

from xbbg.core import utils, assist
from xbbg.io import files, logs, storage

try:
    import pdblp
except ImportError:
    pdblp = utils.load_module(f'{files.abspath(__file__)}/pdblp.py')

_CON_SYM_ = '_xcon_'

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
        all_kw = {
            k: args[n] if n < len(args) else v.default
            for n, (k, v) in enumerate(param.items()) if k != 'kwargs'
        }
        all_kw.update(kwargs)
        log_level = kwargs.get('log', 'info')

        cached_data = []
        if func.__name__ in ['bdp', 'bds']:
            logger = logs.get_logger(func, level=log_level)
            has_date = all_kw.pop('has_date', func.__name__ == 'bds')
            cache = all_kw.get('cache', True)

            tickers = utils.flatten(all_kw['tickers'])
            flds = utils.flatten(all_kw['flds'])
            loaded = pd.DataFrame(data=0, index=tickers, columns=flds)

            for ticker, fld in product(tickers, flds):
                data_file = storage.ref_file(
                    ticker=ticker, fld=fld, has_date=has_date,
                    cache=cache, ext='pkl', **{
                        k: v for k, v in all_kw.items()
                        if k not in ['tickers', 'flds', 'cache', 'raw', 'log']
                    }
                )
                if files.exists(data_file):
                    logger.debug(f'reading from {data_file} ...')
                    cached_data.append(pd.read_pickle(data_file))
                    loaded.loc[ticker, fld] = 1

            to_qry = loaded.where(loaded == 0)\
                .dropna(how='all', axis=1).dropna(how='all', axis=0)

            if to_qry.empty:
                if not cached_data: return pd.DataFrame()
                res = pd.concat(cached_data, sort=False).reset_index(drop=True)
                if not all_kw.get('raw', False):
                    res = assist.format_output(data=res, source=func.__name__)
                return res

            all_kw['tickers'] = to_qry.index.tolist()
            all_kw['flds'] = to_qry.columns.tolist()

        if func.__name__ in ['bdib']:
            data_file = storage.hist_file(
                ticker=all_kw['ticker'], dt=all_kw['dt'], typ=all_kw['typ'],
            )
            if files.exists(data_file):
                logger = logs.get_logger(func, level=log_level)
                if all_kw['batch']: return
                logger.debug(f'reading from {data_file} ...')
                return pd.read_parquet(data_file)

        _, new = create_connection()
        raw = all_kw.pop('raw', False)
        res = func(**all_kw)
        if new: delete_connection()

        if func.__name__ in ['bdp', 'bds']:
            if isinstance(res, list):
                final = cached_data + res
                if not final: return pd.DataFrame()
                res = pd.DataFrame(pd.concat(final, sort=False)).reset_index(drop=True)
            if not raw:
                res = assist.format_output(data=res, source=func.__name__)

        return res
    return wrapper


def create_connection():
    """
    Create Bloomberg connection

    Returns:
        (Bloomberg connection, if connection is new)
    """
    if _CON_SYM_ in globals():
        if not isinstance(globals()[_CON_SYM_], pdblp.BCon):
            del globals()[_CON_SYM_]

    if _CON_SYM_ in globals():
        con = globals()[_CON_SYM_]
        if getattr(con, '_session').start(): con.start()
        return con, False

    else:
        con = pdblp.BCon(port=8194, timeout=30000)
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
