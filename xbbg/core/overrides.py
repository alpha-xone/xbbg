# Set os.environ['BBG_ROOT'] = '/your/bbg/data/path'
#     to enable xbbg saving data locally
BBG_ROOT = 'BBG_ROOT'

PRSV_COLS = [
    'raw', 'has_date', 'cache', 'cache_days', 'col_maps',
    'keep_one', 'price_only', 'port', 'log', 'timeout', 'sess',
]

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
        >>> list(proc_ovrds(DVD_Start_Dt='20180101'))
        [('DVD_Start_Dt', '20180101')]
        >>> list(proc_ovrds(DVD_Start_Dt='20180101', cache=True, has_date=True))
        [('DVD_Start_Dt', '20180101')]
    """
    excluded = list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values()) + PRSV_COLS
    for k, v in kwargs.items():
        if k not in excluded:
            yield k, v


def proc_elms(**kwargs) -> list:
    """
    Bloomberg overrides for elements

    Args:
        **kwargs: overrides

    Returns:
        list of tuples

    Examples:
        >>> list(proc_elms(PerAdj='A', Per='W'))
        [('periodicityAdjustment', 'ACTUAL'), ('periodicitySelection', 'WEEKLY')]
        >>> list(proc_elms(Days='A', Fill='B'))
        [('nonTradingDayFillOption', 'ALL_CALENDAR_DAYS'), ('nonTradingDayFillMethod', 'NIL_VALUE')]
        >>> list(proc_elms(CshAdjNormal=False, CshAdjAbnormal=True))
        [('adjustmentNormal', False), ('adjustmentAbnormal', True)]
        >>> list(proc_elms(Per='W', Quote='Average', start_date='2018-01-10'))
        [('periodicitySelection', 'WEEKLY'), ('overrideOption', 'OVERRIDE_OPTION_GPA')]
        >>> list(proc_elms(QuoteType='Y'))
        [('pricingOption', 'PRICING_OPTION_YIELD')]
        >>> list(proc_elms(QuoteType='Y', cache=True))
        [('pricingOption', 'PRICING_OPTION_YIELD')]
    """
    included = list(ELEM_KEYS.keys()) + list(ELEM_KEYS.values())
    for k, v in kwargs.items():
        if (k in included) and (k not in PRSV_COLS):
            yield ELEM_KEYS.get(k, k), \
                ELEM_VALS.get(ELEM_KEYS.get(k, k), dict()).get(v, v)


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
