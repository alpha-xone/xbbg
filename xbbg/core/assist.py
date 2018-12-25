import pandas as pd

from xbbg.core import utils

# Set os.environ['BBG_ROOT'] = '/your/bbg/data/path'
#     to enable xbbg saving data locally
BBG_ROOT = 'BBG_ROOT'

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
    """
    return [
        (k, v) for k, v in kwargs.items()
        if k not in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values())
    ]


def proc_elms(**kwargs):
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
    """
    return [
        (ELEM_KEYS.get(k, k), ELEM_VALS.get(ELEM_KEYS.get(k, k), dict()).get(v, v))
        for k, v in kwargs.items()
        if k in list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values())
    ]


def info_key(ticker: str, dt, typ='TRADE', **kwargs):
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


def format_earning(data: pd.DataFrame, header: pd.DataFrame):
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
                         Level  FY_2017  FY_2017_Pct
        Asia-Pacific       1.0   3540.0        66.43
           China           2.0   1747.0        49.35
           Japan           2.0   1242.0        35.08
           Singapore       2.0    551.0        15.56
        United States      1.0   1364.0        25.60
        Europe             1.0    263.0         4.94
        Other Countries    1.0    162.0         3.04
    """
    if data.dropna(subset=['value']).empty: return pd.DataFrame()

    res = pd.concat([
        grp.loc[:, ['value']].set_index(header.value)
        for _, grp in data.groupby(data.position)
    ], axis=1)
    res.columns = res.iloc[0]
    res.index.name = None
    res = res.iloc[1:].transpose().reset_index().apply(
        pd.to_numeric, downcast='float', errors='ignore'
    )
    res.rename(columns=lambda vv: '_'.join(vv.split()), inplace=True)

    years = res.columns[res.columns.str.startswith('FY_')]
    lvl_1 = res.Level == 1
    for yr in years:
        res.loc[:, yr] = res.loc[:, yr].round(1)
        pct = f'{yr}_Pct'
        res.loc[:, pct] = 0.
        res.loc[lvl_1, pct] = res.loc[lvl_1, pct].astype(float).round(1)
        res.loc[lvl_1, pct] = res.loc[lvl_1, yr] / res.loc[lvl_1, yr].sum() * 100
        sub_pct = []
        for _, snap in res[::-1].iterrows():
            if snap.Level > 2: continue
            if snap.Level == 1:
                if len(sub_pct) == 0: continue
                sub = pd.concat(sub_pct, axis=1).transpose()
                res.loc[sub.index, pct] = \
                    res.loc[sub.index, yr] / res.loc[sub.index, yr].sum() * 100
                sub_pct = []
            if snap.Level == 2: sub_pct.append(snap)

    res.set_index('Segment_Name', inplace=True)
    res.index.name = None
    return res


def format_output(data: pd.DataFrame, source, col_maps=None):
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
        ... )
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
            pd.Series({**{'Ticker': t}, **grp.set_index('field').value.to_dict()})
            for t, grp in data.groupby('ticker')
        ], axis=1, sort=False)).transpose()
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


def info_qry(tickers, flds):
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
