import pandas as pd

from itertools import product
from collections import namedtuple

from xbbg.core import utils
from xbbg.io import files, logs, storage

ToQuery = namedtuple('ToQuery', ['tickers', 'flds', 'cached_data'])
EXC_COLS = ['tickers', 'flds', 'raw', 'log', 'col_maps']


def bdp_bds_cache(func, tickers, flds, **kwargs) -> ToQuery:
    """
    Find cached `BDP` / `BDS` queries

    Args:
        func: function name - bdp or bds
        tickers: tickers
        flds: fields
        **kwargs: other kwargs

    Returns:
        ToQuery(ticker, flds, kwargs)
    """
    cache_data = []
    logger = logs.get_logger(bdp_bds_cache, **kwargs)
    kwargs['has_date'] = kwargs.pop('has_date', func == 'bds')
    kwargs['cache'] = kwargs.get('cache', True)

    tickers = utils.flatten(tickers)
    flds = utils.flatten(flds)
    loaded = pd.DataFrame(data=0, index=tickers, columns=flds)

    for ticker, fld in product(tickers, flds):
        data_file = storage.ref_file(
            ticker=ticker, fld=fld, ext='pkl', **{
                k: v for k, v in kwargs.items() if k not in EXC_COLS
            }
        )
        if not files.exists(data_file): continue
        logger.debug(f'reading from {data_file} ...')
        cache_data.append(pd.read_pickle(data_file))
        loaded.loc[ticker, fld] = 1

    to_qry = loaded.where(loaded == 0)\
        .dropna(how='all', axis=1).dropna(how='all', axis=0)

    return ToQuery(
        tickers=to_qry.index.tolist(), flds=to_qry.columns.tolist(),
        cached_data=cache_data
    )
