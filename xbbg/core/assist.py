import pandas as pd

from xbbg.core import utils

# Set os.environ['BBG_ROOT'] = '/your/bbg/data/path'
#     to enable xbbg saving data locally
BBG_ROOT = 'BBG_ROOT'

PRSV_COLS = ['raw', 'has_date', 'cache', 'cache_days', 'col_maps']

ELEMENTS = [
    'periodicityAdjustment', 'periodicitySelection', 'currency',
    'nonTradingDayFillOption', 'nonTradingDayFillMethod',
    'maxDataPoints', 'returnEIDs', 'returnRelativeDate',
    'overrideOption', 'pricingOption',
    'adjustmentNormal', 'adjustmentAbnormal', 'adjustmentSplit',
    'adjustmentFollowDPDF', 'calendarCodeOverride',
]

ELEM_KEYS = dict(
    PeriodAdj='periodicityAdjustment', PerAdj='periodicityAdjustment',
    Period='periodicitySelection', Per='periodicitySelection',
    Currency='currency', Curr='currency', FX='currency',
    Days='nonTradingDayFillOption', Fill='nonTradingDayFillMethod', Points='maxDataPoints',
    # 'returnEIDs', 'returnRelativeDate',
    Quote='overrideOption', QuoteType='pricingOption', QtTyp='pricingOption',
    CshAdjNormal='adjustmentNormal', CshAdjAbnormal='adjustmentAbnormal',
    CapChg='adjustmentSplit', UseDPDF='adjustmentFollowDPDF',
    Calendar='calendarCodeOverride',
)

ELEM_VALS = dict(
    periodicityAdjustment=dict(
        A='ACTUAL', C='CALENDAR', F='FISCAL',
    ),
    periodicitySelection=dict(
        D='DAILY', W='WEEKLY', M='MONTHLY', Q='QUARTERLY', S='SEMI_ANNUALLY', Y='YEARLY'
    ),
    nonTradingDayFillOption=dict(
        N='NON_TRADING_WEEKDAYS', W='NON_TRADING_WEEKDAYS', Weekdays='NON_TRADING_WEEKDAYS',
        C='ALL_CALENDAR_DAYS', A='ALL_CALENDAR_DAYS', All='ALL_CALENDAR_DAYS',
        T='ACTIVE_DAYS_ONLY', Trading='ACTIVE_DAYS_ONLY',
    ),
    nonTradingDayFillMethod=dict(
        C='PREVIOUS_VALUE', P='PREVIOUS_VALUE', Previous='PREVIOUS_VALUE',
        B='NIL_VALUE', Blank='NIL_VALUE', NA='NIL_VALUE',
    ),
    overrideOption=dict(
        A='OVERRIDE_OPTION_GPA', G='OVERRIDE_OPTION_GPA', Average='OVERRIDE_OPTION_GPA',
        C='OVERRIDE_OPTION_CLOSE', Close='OVERRIDE_OPTION_CLOSE',
    ),
    pricingOption=dict(
        P='PRICING_OPTION_PRICE', Price='PRICING_OPTION_PRICE',
        Y='PRICING_OPTION_YIELD', Yield='PRICING_OPTION_YIELD',
    ),
)


def proc_ovrds(**kwargs):
    """
    Bloomberg overrides

    Args:
        **kwargs: overrides

    Returns:
        list of tuples

    Examples:
        >>> proc_ovrds(DVD_Start_Dt='20180101')
        [('DVD_Start_Dt', '20180101')]
        >>> proc_ovrds(DVD_Start_Dt='20180101', cache=True, has_date=True)
        [('DVD_Start_Dt', '20180101')]
    """
    return [
        (k, v) for k, v in kwargs.items()
        if k not in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values()) + PRSV_COLS
    ]


def proc_elms(**kwargs) -> list:
    """
    Bloomberg overrides for elements

    Args:
        **kwargs: overrides

    Returns:
        list of tuples

    Examples:
        >>> proc_elms(PerAdj='A', Per='W')
        [('periodicityAdjustment', 'ACTUAL'), ('periodicitySelection', 'WEEKLY')]
        >>> proc_elms(Days='A', Fill='B')
        [('nonTradingDayFillOption', 'ALL_CALENDAR_DAYS'), ('nonTradingDayFillMethod', 'NIL_VALUE')]
        >>> proc_elms(CshAdjNormal=False, CshAdjAbnormal=True)
        [('adjustmentNormal', False), ('adjustmentAbnormal', True)]
        >>> proc_elms(Per='W', Quote='Average', start_date='2018-01-10')
        [('periodicitySelection', 'WEEKLY'), ('overrideOption', 'OVERRIDE_OPTION_GPA')]
        >>> proc_elms(QuoteType='Y')
        [('pricingOption', 'PRICING_OPTION_YIELD')]
        >>> proc_elms(QuoteType='Y', cache=True)
        [('pricingOption', 'PRICING_OPTION_YIELD')]
    """
    return [
        (ELEM_KEYS.get(k, k), ELEM_VALS.get(ELEM_KEYS.get(k, k), dict()).get(v, v))
        for k, v in kwargs.items()
        if (k in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values()))
        and (k not in PRSV_COLS)
    ]


def info_key(ticker: str, dt, typ='TRADE', **kwargs) -> str:
    """
    Generate key from given info

    Args:
        ticker: ticker name
        dt: date to download
        typ: [TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID, BEST_ASK]
        **kwargs: other kwargs

    Returns:
        str

    Examples:
        >>> info_key('X US Equity', '2018-02-01', OtherInfo='Any')
        '{ticker=X US Equity, dt=2018-02-01, typ=TRADE, OtherInfo=Any}'
    """
    return utils.to_str(dict(
        ticker=ticker, dt=pd.Timestamp(dt).strftime('%Y-%m-%d'), typ=typ, **kwargs
    ))


def format_earning(data: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    """
    Standardized earning outputs and add percentage by each blocks

    Args:
        data: earning data block
        header: earning headers

    Returns:
        pd.DataFrame

    Examples:
        >>> format_earning(
        ...     data=pd.read_pickle('xbbg/tests/data/sample_earning.pkl'),
        ...     header=pd.read_pickle('xbbg/tests/data/sample_earning_header.pkl')
        ... ).round(2)
                         level  fy2017  fy2017_pct
        Asia-Pacific       1.0  3540.0       66.43
           China           2.0  1747.0       49.35
           Japan           2.0  1242.0       35.08
           Singapore       2.0   551.0       15.56
        United States      1.0  1364.0       25.60
        Europe             1.0   263.0        4.94
        Other Countries    1.0   162.0        3.04
    """
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    res = pd.concat([
        grp.loc[:, ['value']].set_index(header.value)
        for _, grp in data.groupby(data.position)
    ], axis=1)
    res.index.name = None
    res.columns = res.iloc[0]
    res = res.iloc[1:].transpose().reset_index().apply(
        pd.to_numeric, downcast='float', errors='ignore'
    )
    res.rename(
        columns=lambda vv: '_'.join(vv.lower().split()).replace('fy_', 'fy'),
        inplace=True,
    )

    years = res.columns[res.columns.str.startswith('fy')]
    lvl_1 = res.level == 1
    for yr in years:
        res.loc[:, yr] = res.loc[:, yr].round(1)
        pct = f'{yr}_pct'
        res.loc[:, pct] = 0.
        res.loc[lvl_1, pct] = res.loc[lvl_1, pct].astype(float).round(1)
        res.loc[lvl_1, pct] = res.loc[lvl_1, yr] / res.loc[lvl_1, yr].sum() * 100
        sub_pct = []
        for _, snap in res[::-1].iterrows():
            if snap.level > 2: continue
            if snap.level == 1:
                if len(sub_pct) == 0: continue
                sub = pd.concat(sub_pct, axis=1).transpose()
                res.loc[sub.index, pct] = \
                    res.loc[sub.index, yr] / res.loc[sub.index, yr].sum() * 100
                sub_pct = []
            if snap.level == 2: sub_pct.append(snap)

    res.set_index('segment_name', inplace=True)
    res.index.name = None
    return res


def format_output(data: pd.DataFrame, source, col_maps=None) -> pd.DataFrame:
    """
    Format `pdblp` outputs to column-based results

    Args:
        data: `pdblp` result
        source: `bdp` or `bds`
        col_maps: rename columns with these mappings

    Returns:
        pd.DataFrame

    Examples:
        >>> format_output(
        ...     data=pd.read_pickle('xbbg/tests/data/sample_bdp.pkl'),
        ...     source='bdp'
        ... ).reset_index()
                  ticker                        name
        0  QQQ US Equity  INVESCO QQQ TRUST SERIES 1
        1  SPY US Equity      SPDR S&P 500 ETF TRUST
        >>> format_output(
        ...     data=pd.read_pickle('xbbg/tests/data/sample_dvd.pkl'),
        ...     source='bds', col_maps={'Dividend Frequency': 'dvd_freq'}
        ... ).loc[:, ['ex_date', 'dividend_amount', 'dvd_freq']].reset_index()
                ticker     ex_date  dividend_amount dvd_freq
        0  C US Equity  2018-02-02             0.32  Quarter
    """
    if data.empty: return pd.DataFrame()
    if source == 'bdp': req_cols = ['ticker', 'field', 'value']
    else: req_cols = ['ticker', 'field', 'name', 'value', 'position']
    if any(col not in data for col in req_cols): return pd.DataFrame()
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    if source == 'bdp':
        res = pd.DataFrame(pd.concat([
            pd.Series({**{'ticker': t}, **grp.set_index('field').value.to_dict()})
            for t, grp in data.groupby('ticker')
        ], axis=1, sort=False)).transpose().set_index('ticker')
    else:
        res = pd.DataFrame(pd.concat([
            grp.loc[:, ['name', 'value']].set_index('name')
            .transpose().reset_index(drop=True).assign(ticker=t)
            for (t, _), grp in data.groupby(['ticker', 'position'])
        ], sort=False)).reset_index(drop=True).set_index('ticker')
        res.columns.name = None

    if col_maps is None: col_maps = dict()
    return res.rename(
        columns=lambda vv: col_maps.get(
            vv, vv.lower().replace(' ', '_').replace('-', '_')
        )
    ).apply(pd.to_numeric, errors='ignore', downcast='float')


def format_intraday(data: pd.DataFrame, ticker) -> pd.DataFrame:
    """
    Format intraday data

    Args:
        data: pd.DataFrame from bdib
        ticker: ticker

    Returns:
        pd.DataFrame

    Examples:
        >>> format_intraday(
        ...     data=pd.read_parquet('xbbg/tests/data/sample_bdib.parq'),
        ...     ticker='SPY US Equity',
        ... )[[('SPY US Equity', 'close')]]
        ticker                    SPY US Equity
        field                             close
        2018-12-28 09:30:00-05:00        249.67
        2018-12-28 09:31:00-05:00        249.54
        2018-12-28 09:32:00-05:00        249.22
        2018-12-28 09:33:00-05:00        249.01
        2018-12-28 09:34:00-05:00        248.86
    """
    if data.empty: return pd.DataFrame()
    data.columns = pd.MultiIndex.from_product([
        [ticker], data.rename(columns=dict(numEvents='num_trds')).columns
    ], names=['ticker', 'field'])
    return data


def info_qry(tickers, flds) -> str:
    """
    Logging info for given tickers and fields

    Args:
        tickers: tickers
        flds: fields

    Returns:
        str

    Examples:
        >>> print(info_qry(
        ...     tickers=['NVDA US Equity'], flds=['Name', 'Security_Name']
        ... ))
        tickers: ['NVDA US Equity']
        fields:  ['Name', 'Security_Name']
    """
    full_list = '\n'.join([f'tickers: {tickers[:8]}'] + [
        f'         {tickers[n:(n + 8)]}' for n in range(8, len(tickers), 8)
    ])
    return f'{full_list}\nfields:  {flds}'
