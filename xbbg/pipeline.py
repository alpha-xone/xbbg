import pandas as pd
import numpy as np


def get_series(data: (pd.Series, pd.DataFrame), col='close') -> pd.DataFrame:
    """
    Get close column from intraday data

    Args:
        data: intraday data
        col: column to return

    Returns:
        pd.Series or pd.DataFrame
    """
    if isinstance(data, pd.Series): return pd.DataFrame(data)
    if not isinstance(data.columns, pd.MultiIndex): return data
    return data.xs(col, axis=1, level=1)


def clean_cols(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean column name
    """
    data.columns.name = None
    return data


def standard_cols(data: pd.DataFrame, col_maps: dict = None) -> pd.DataFrame:
    """
    Rename data columns to snake case

    Args:
        data: input data
        col_maps: column maps

    Returns:
        pd.DataFrame
    """
    if col_maps is None: col_maps = dict()
    return data.rename(
        columns=lambda vv: col_maps.get(
            vv, vv.lower().replace(' ', '_').replace('-', '_')
        )
    )


def apply_fx(
        data: (pd.Series, pd.DataFrame),
        fx: (int, float, pd.Series, pd.DataFrame), power=-1.
) -> pd.DataFrame:
    """
    Apply FX to data

    Args:
        data: price data
        fx: FX price data
        power: apply for FX price

    Returns:
        Price * FX ** Power
        where FX uses latest available price
    """
    if isinstance(data, pd.Series): data = pd.DataFrame(data)

    if isinstance(fx, (int, float)):
        return data.dropna(how='all').mul(fx ** power)

    add_fx = pd.concat([data, fx.pipe(get_series).iloc[:, -1]], axis=1)
    add_fx.iloc[:, -1] = add_fx.iloc[:, -1].fillna(method='pad')
    return data.mul(add_fx.iloc[:, -1].pow(power), axis=0).dropna(how='all')


def daily_stats(data: (pd.Series, pd.DataFrame), **kwargs) -> pd.DataFrame:
    """
    Daily stats for given data
    """
    if data.empty: return pd.DataFrame()
    if 'percentiles' not in kwargs: kwargs['percentiles'] = [.1, .25, .5, .75, .9]
    return data.groupby(data.index.floor('d')).describe(**kwargs)


def dropna(data: (pd.Series, pd.DataFrame), cols: (int, list) = 0) -> (pd.Series, pd.DataFrame):
    """
    Drop NAs by columns
    """
    if isinstance(data, pd.Series): return data.dropna()
    if isinstance(cols, int): cols = [cols]
    return data.dropna(how='all', subset=data.columns[cols])


def to_numeric(data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert data to numeric if possible
    """
    return data.apply(pd.to_numeric, errors='ignore')


def format_raw(data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert data to datetime if possible
    """
    res = data.apply(pd.to_numeric, errors='ignore')
    dtypes = data.dtypes
    cols = dtypes.loc[
        dtypes.isin([np.dtype('O')]) | data.columns.str.contains('UPDATE_STAMP')
    ].index
    if not cols.empty:
        res.loc[:, cols] = data.loc[:, cols].apply(pd.to_datetime, errors='ignore')
    return data


def add_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Add ticker as first layer of multi-index

    Args:
        data: raw data
        ticker: ticker

    Returns:
        pd.DataFrame
    """
    data.columns = pd.MultiIndex.from_product([
        [ticker], data.head().rename(columns={'numEvents': 'num_trds'}).columns
    ])
    return data


def since_year(data: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Remove columns prior to give year.
    To make this work, column names must contian year explicitly.

    Args:
        data: raw data
        year: starting year

    Returns:
        pd.DataFrame
    """
    return data.loc[:, ~data.columns.str.contains(
        '|'.join(map(str, range(year, year - 20, -1)))
    )]
