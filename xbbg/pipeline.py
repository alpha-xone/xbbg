import pandas as pd
import numpy as np

from typing import Union


def get_series(data: Union[pd.Series, pd.DataFrame], col='close') -> pd.DataFrame:
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


def standard_cols(data: pd.DataFrame, col_maps: dict = None) -> pd.DataFrame:
    """
    Rename data columns to snake case

    Args:
        data: input data
        col_maps: column maps

    Returns:
        pd.DataFrame

    Examples:
        >>> dvd = pd.read_pickle('xbbg/tests/data/sample_dvd_mc_raw.pkl').iloc[:, :4]
        >>> dvd
                     Declared Date     Ex-Date Record Date Payable Date
        MC FP Equity    2019-07-24  2019-12-06  2019-12-09   2019-12-10
        MC FP Equity    2019-01-29  2019-04-25  2019-04-26   2019-04-29
        MC FP Equity    2018-07-24  2018-12-04  2018-12-05   2018-12-06
        MC FP Equity    2018-01-25  2018-04-17  2018-04-18   2018-04-19
        >>> dvd.pipe(standard_cols)
                     declared_date     ex_date record_date payable_date
        MC FP Equity    2019-07-24  2019-12-06  2019-12-09   2019-12-10
        MC FP Equity    2019-01-29  2019-04-25  2019-04-26   2019-04-29
        MC FP Equity    2018-07-24  2018-12-04  2018-12-05   2018-12-06
        MC FP Equity    2018-01-25  2018-04-17  2018-04-18   2018-04-19
        >>> dvd.pipe(standard_cols, col_maps={'Declared Date': 'dec_date'})
                        dec_date     ex_date record_date payable_date
        MC FP Equity  2019-07-24  2019-12-06  2019-12-09   2019-12-10
        MC FP Equity  2019-01-29  2019-04-25  2019-04-26   2019-04-29
        MC FP Equity  2018-07-24  2018-12-04  2018-12-05   2018-12-06
        MC FP Equity  2018-01-25  2018-04-17  2018-04-18   2018-04-19
    """
    if col_maps is None: col_maps = dict()
    return data.rename(
        columns=lambda vv: col_maps.get(
            vv, vv.lower().replace(' ', '_').replace('-', '_')
        )
    )


def apply_fx(
        data: Union[pd.Series, pd.DataFrame],
        fx: Union[int, float, pd.Series, pd.DataFrame],
        power=-1.,
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

    Examples:
        >>> pd.options.display.precision = 2
        >>> rms = (
        ...     pd.read_pickle('xbbg/tests/data/sample_rms_ib1.pkl')
        ...     .pipe(get_series, col='close')
        ...     .apply(pd.to_numeric, errors='ignore')
        ...     .rename_axis(columns=None)
        ...     .pipe(dropna)
        ... ).tail()
        >>> eur = pd.read_pickle('xbbg/tests/data/sample_eur_ib.pkl')
        >>> rms
                                   RMS FP Equity
        2020-01-17 16:26:00+00:00          725.4
        2020-01-17 16:27:00+00:00          725.2
        2020-01-17 16:28:00+00:00          725.4
        2020-01-17 16:29:00+00:00          725.0
        2020-01-17 16:35:00+00:00          725.6
        >>> rms.iloc[:, 0].pipe(apply_fx, fx=eur)
                                   RMS FP Equity
        2020-01-17 16:26:00+00:00         653.98
        2020-01-17 16:27:00+00:00         653.80
        2020-01-17 16:28:00+00:00         653.98
        2020-01-17 16:29:00+00:00         653.57
        2020-01-17 16:35:00+00:00         654.05
        >>> rms.pipe(apply_fx, fx=1.1090)
                                   RMS FP Equity
        2020-01-17 16:26:00+00:00         654.10
        2020-01-17 16:27:00+00:00         653.92
        2020-01-17 16:28:00+00:00         654.10
        2020-01-17 16:29:00+00:00         653.74
        2020-01-17 16:35:00+00:00         654.28
    """
    if isinstance(data, pd.Series): data = pd.DataFrame(data)

    if isinstance(fx, (int, float)):
        return data.dropna(how='all').mul(fx ** power)

    add_fx = pd.concat([data, fx.pipe(get_series).iloc[:, -1]], axis=1)
    add_fx.iloc[:, -1] = add_fx.iloc[:, -1].fillna(method='pad')
    return data.mul(add_fx.iloc[:, -1].pow(power), axis=0).dropna(how='all')


def daily_stats(data: Union[pd.Series, pd.DataFrame], **kwargs) -> pd.DataFrame:
    """
    Daily stats for given data

    Examples:
        >>> pd.options.display.precision = 2
        >>> (
        ...     pd.concat([
        ...         pd.read_pickle('xbbg/tests/data/sample_rms_ib0.pkl'),
        ...         pd.read_pickle('xbbg/tests/data/sample_rms_ib1.pkl'),
        ...     ], sort=False)
        ...     .pipe(get_series, col='close')
        ...     .pipe(daily_stats)
        ... )['RMS FP Equity'].iloc[:, :5]
                                   count    mean   std    min    10%
        2020-01-16 00:00:00+00:00  434.0  711.16  1.11  708.6  709.6
        2020-01-17 00:00:00+00:00  437.0  721.53  1.66  717.0  719.0
    """
    if data.empty: return pd.DataFrame()
    if 'percentiles' not in kwargs: kwargs['percentiles'] = [.1, .25, .5, .75, .9]
    return data.groupby(data.index.floor('d')).describe(**kwargs)


def dropna(
        data: Union[pd.Series, pd.DataFrame],
        cols: Union[int, list] = 0,
) -> Union[pd.Series, pd.DataFrame]:
    """
    Drop NAs by columns
    """
    if isinstance(data, pd.Series): return data.dropna()
    if isinstance(cols, int): cols = [cols]
    return data.dropna(how='all', subset=data.columns[cols])


def format_raw(data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert data to datetime if possible

    Examples:
        >>> dvd = pd.read_pickle('xbbg/tests/data/sample_dvd_mc_raw.pkl')
        >>> dvd.dtypes
        Declared Date          object
        Ex-Date                object
        Record Date            object
        Payable Date           object
        Dividend Amount       float64
        Dividend Frequency     object
        Dividend Type          object
        dtype: object
        >>> dvd.pipe(format_raw).dtypes
        Declared Date         datetime64[ns]
        Ex-Date               datetime64[ns]
        Record Date           datetime64[ns]
        Payable Date          datetime64[ns]
        Dividend Amount              float64
        Dividend Frequency            object
        Dividend Type                 object
        dtype: object
    """
    res = data.apply(pd.to_numeric, errors='ignore')
    dtypes = data.dtypes
    cols = dtypes.loc[
        dtypes.isin([np.dtype('O')]) | data.columns.str.contains('UPDATE_STAMP')
    ].index
    if not cols.empty:
        res.loc[:, cols] = data.loc[:, cols].apply(pd.to_datetime, errors='ignore')
    return res


def add_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Add ticker as first layer of multi-index

    Args:
        data: raw data
        ticker: ticker

    Returns:
        pd.DataFrame

    Examples:
        >>> (
        ...     pd.read_parquet('xbbg/tests/data/sample_bdib.parq')
        ...     .pipe(add_ticker, ticker='SPY US Equity')
        ...     .pipe(get_series, col='close')
        ... )
                                   SPY US Equity
        2018-12-28 09:30:00-05:00         249.67
        2018-12-28 09:31:00-05:00         249.54
        2018-12-28 09:32:00-05:00         249.22
        2018-12-28 09:33:00-05:00         249.01
        2018-12-28 09:34:00-05:00         248.86
    """
    data.columns = pd.MultiIndex.from_product([
        [ticker], data.head().rename(columns={'numEvents': 'num_trds'}).columns
    ])
    return data


def since_year(data: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Remove columns prior to give year.
    To make this work, column names must contain the year explicitly.

    Args:
        data: raw data
        year: starting year

    Returns:
        pd.DataFrame

    Examples:
        >>> pd.options.display.width = 120
        >>> pd.options.display.max_columns = 10
        >>> pd.options.display.precision = 2
        >>> amzn = pd.read_pickle('xbbg/tests/data/sample_earning_amzn.pkl')
        >>> amzn.query('level == 1').pipe(since_year, year=2017)
                         segment_name  level    fy2018    fy2017  fy2018_pct  fy2017_pct
        AMZN US Equity  North America      1  141366.0  106110.0       60.70       59.66
        AMZN US Equity  International      1   65866.0   54297.0       28.28       30.53
        AMZN US Equity            AWS      1   25655.0   17459.0       11.02        9.82
        >>> amzn.query('level == 1').pipe(since_year, year=2018)
                         segment_name  level    fy2018  fy2018_pct
        AMZN US Equity  North America      1  141366.0       60.70
        AMZN US Equity  International      1   65866.0       28.28
        AMZN US Equity            AWS      1   25655.0       11.02
    """
    return data.loc[:, ~data.columns.str.contains(
        '|'.join(map(str, range(year - 20, year)))
    )]


def perf(data: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
    """
    Performance rebased to 100

    Examples:
        >>> (
        ...     pd.DataFrame({
        ...         's1': [1., np.nan, 1.01, 1.03, .99],
        ...         's2': [np.nan, 1., .99, 1.04, 1.1],
        ...     })
        ...     .pipe(perf)
        ... )
              s1     s2
        0  100.0    NaN
        1    NaN  100.0
        2  101.0   99.0
        3  103.0  104.0
        4   99.0  110.0
    """
    if isinstance(data, pd.Series):
        return (
            data
            .dropna()
            .pct_change()
            .fillna(0)
            .add(1)
            .cumprod()
            .mul(100)
        )
    return data.apply(perf, axis=0)
