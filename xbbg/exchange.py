from collections import namedtuple
from xbbg.core.timezone import TimeZone

Session = namedtuple('Session', ['start_time', 'end_time'])
MarketSessions = namedtuple('MarketSessions', [
    'allday', 'day', 'am', 'pm', 'night', 'pre', 'post',
])
TradingHours = namedtuple('TradingHours', ['hours', 'tz'])

SessNA = Session(None, None)


class ExchHours(dict):
    """
    Exchange market hours
    """

    # ========================== #
    #           Equity           #
    # ========================== #

    EquityAustralia = TradingHours(
        hours=MarketSessions(
            allday=Session('09:59', '16:16'), day=Session('10:00', '16:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:01', '16:16'),
        ),
        tz=TimeZone.AU,
    )

    EquityJapan = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '15:45'), day=Session('09:01', '14:58'),
            am=Session('09:01', '11:30'), pm=Session('12:30', '14:58'),
            night=SessNA, pre=Session('09:00', '09:00'), post=Session('14:59', '15:45'),
        ),
        tz=TimeZone.JP,
    )

    EquitySouthKorea = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '15:35'), day=Session('09:00', '15:20'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('15:21', '15:35'),
        ),
        tz=TimeZone.SK,
    )

    EquityTaiwan = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '13:35'), day=Session('09:00', '13:25'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('13:26', '13:35'),
        ),
        tz=TimeZone.TW,
    )

    EquityHongKong = TradingHours(
        hours=MarketSessions(
            allday=Session('09:15', '16:15'), day=Session('09:30', '16:00'),
            am=Session('09:30', '12:00'), pm=Session('13:00', '16:00'),
            night=SessNA, pre=Session('09:15', '09:30'), post=Session('16:01', '16:15'),
        ),
        tz=TimeZone.HK,
    )

    EquityChina = TradingHours(
        hours=MarketSessions(
            allday=Session('09:15', '15:05'), day=Session('09:30', '15:00'),
            am=Session('09:30', '11:30'), pm=Session('13:00', '15:00'),
            night=SessNA, pre=Session('09:15', '09:30'), post=SessNA,
        ),
        tz=TimeZone.SH,
    )

    EquityIndia = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '17:10'), day=Session('09:00', '15:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('15:31', '17:10'),
        ),
        tz=TimeZone.IN,
    )

    EquityLondon = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '17:00'),
        ),
        tz=TimeZone.UK,
    )

    EquityDublin = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '17:00'),
        ),
        tz=TimeZone.UK,
    )

    EquityAmsterdam = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '17:00'),
        ),
        tz=TimeZone.UK,
    )

    EquitySpain = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '17:00'),
        ),
        tz=TimeZone.UK,
    )

    EquityFrance = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '17:00'),
        ),
        tz=TimeZone.UK,
    )

    EquityUS = TradingHours(
        hours=MarketSessions(
            allday=Session('04:00', '20:00'), day=Session('09:30', '16:00'),
            am=SessNA, pm=SessNA, night=SessNA,
            pre=Session('04:00', '09:30'), post=Session('16:01', '20:00'),
        ),
        tz=TimeZone.NY,
    )

    # ============================ #
    #           Currency           #
    # ============================ #

    CurrencyGeneric = TradingHours(
        hours=MarketSessions(
            allday=Session('17:01', '17:00'), day=Session('17:01', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    CurrencySouthKorea = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '15:30'), day=Session('09:00', '15:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SK,
    )

    CurrencyTaiwan = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '16:00'), day=Session('09:00', '16:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.TW,
    )

    CurrencyChina = TradingHours(
        hours=MarketSessions(
            allday=Session('09:30', '23:30'), day=Session('09:30', '23:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SH,
    )

    CurrencyIndia = TradingHours(
        hours=MarketSessions(
            allday=Session('09:00', '17:00'), day=Session('09:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.IN,
    )

    CurrencyDubai = TradingHours(
        hours=MarketSessions(
            allday=Session('07:00', '23:59'), day=Session('07:00', '23:59'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.DB,
    )

    CurrencySingapore = TradingHours(
        hours=MarketSessions(
            allday=Session('19:50', '19:35'), day=Session('07:25', '19:35'),
            am=SessNA, pm=SessNA, night=Session('19:50', '04:45'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SG,
    )

    CurrencyICE = TradingHours(
        hours=MarketSessions(
            allday=Session('18:30', '17:30'), day=Session('18:30', '17:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    # =================================== #
    #           Index / Futures           #
    # =================================== #

    EquityFuturesIndia = TradingHours(
        hours=MarketSessions(
            allday=Session('09:15', '16:45'), day=Session('09:15', '15:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('15:31', '16:45'),
        ),
        tz=TimeZone.IN,
    )

    IndexFuturesIndia = TradingHours(
        hours=MarketSessions(
            allday=Session('09:15', '15:30'), day=Session('09:15', '15:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.IN,
    )

    FuturesAustralia = TradingHours(
        hours=MarketSessions(
            allday=Session('17:10', '16:30'), day=Session('09:50', '16:30'),
            am=SessNA, pm=SessNA, night=Session('17:10', '07:00'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.AU,
    )

    IndexAustralia = TradingHours(
        hours=MarketSessions(
            allday=Session('10:00', '16:20'), day=Session('10:00', '16:20'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.AU,
    )

    FuturesJapan = TradingHours(
        hours=MarketSessions(
            allday=Session('16:30', '15:15'), day=Session('08:45', '15:15'),
            am=SessNA, pm=SessNA, night=Session('16:30', '05:30'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.JP,
    )

    FuturesSouthKorea = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '15:45'), day=Session('09:00', '15:45'),
            am=SessNA, pm=SessNA, night=Session('18:00', '05:00'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SK,
    )

    IndexSouthKorea = TradingHours(
        hours=MarketSessions(
            allday=Session('08:30', '15:35'), day=Session('09:00', '15:20'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('15:21', '15:35'),
        ),
        tz=TimeZone.SK,
    )

    FuturesTaiwan = TradingHours(
        hours=MarketSessions(
            allday=Session('14:15', '13:50'), day=Session('08:45', '13:50'),
            am=SessNA, pm=SessNA, night=Session('14:15', '04:50'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.TW,
    )

    FuturesHongKong = TradingHours(
        hours=MarketSessions(
            allday=Session('17:15', '01:00'), day=Session('09:15', '16:30'),
            am=SessNA, pm=SessNA, night=Session('17:15', '01:00'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.HK,
    )

    FuturesSingapore = TradingHours(
        hours=MarketSessions(
            allday=Session('17:00', '04:45'), day=Session('09:00', '16:35'),
            am=SessNA, pm=SessNA, night=Session('17:00', '04:45'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SG,
    )

    IndexLondon = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '16:35'), day=Session('08:00', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=Session('16:31', '16:35'),
        ),
        tz=TimeZone.UK,
    )

    IndexEurope1 = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:00'), day=Session('08:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.UK,
    )

    IndexEurope2 = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '17:15'), day=Session('08:00', '17:15'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.UK,
    )

    IndexEurope3 = TradingHours(
        hours=MarketSessions(
            allday=Session('08:00', '18:30'), day=Session('08:00', '18:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.UK,
    )

    IndexUS = TradingHours(
        hours=MarketSessions(
            allday=Session('09:30', '16:00'), day=Session('09:30', '16:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    CME = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '17:00'), day=Session('08:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    FuturesFinancialsICE = TradingHours(
        hours=MarketSessions(
            allday=Session('01:00', '21:00'), day=Session('01:00', '21:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.UK,
    )

    FuturesNYFICE = TradingHours(
        hours=MarketSessions(
            allday=Session('20:00', '18:00'), day=Session('20:00', '18:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    IndexVIX = TradingHours(
        hours=MarketSessions(
            allday=Session('03:00', '16:30'), day=Session('03:15', '16:30'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    FuturesCBOE = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '17:00'), day=Session('18:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    IndexYieldCurve = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '17:20'), day=Session('18:00', '17:20'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    # =============================== #
    #           Commodities           #
    # =============================== #

    NYME = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '17:00'), day=Session('18:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    FuturesEuropeICE = TradingHours(
        hours=MarketSessions(
            allday=Session('01:00', '23:00'), day=Session('01:00', '23:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.UK,
    )

    CMX = TradingHours(
        hours=MarketSessions(
            allday=Session('18:00', '17:00'), day=Session('18:00', '17:00'),
            am=SessNA, pm=SessNA, night=SessNA, pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.NY,
    )

    CommoditiesShanghai = TradingHours(
        hours=MarketSessions(
            allday=Session('21:00', '15:00'), day=Session('09:00', '15:00'),
            am=Session('09:00', '11:30'), pm=Session('13:30', '15:00'),
            night=Session('21:00', '23:00'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SH,
    )

    CommoditiesDalian = TradingHours(
        hours=MarketSessions(
            allday=Session('21:00', '15:00'), day=Session('09:00', '15:00'),
            am=Session('09:00', '11:30'), pm=Session('13:30', '15:00'),
            night=Session('21:00', '23:30'), pre=SessNA, post=SessNA,
        ),
        tz=TimeZone.SH,
    )
