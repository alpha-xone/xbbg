from collections import namedtuple

CurrencyPair = namedtuple('CurrencyPair', ['ticker', 'factor', 'reversal'])
Currencies = {
    'AUDUSD': CurrencyPair('AUD Curncy', 1., -1),

    'JPYUSD': CurrencyPair('JPY Curncy', 1., 1),

    'KRWUSD': CurrencyPair('KRW Curncy', 1., 1),
    'KWN+1MUSD': CurrencyPair('KRW+1M Curncy', 1., 1),

    'TWDUSD': CurrencyPair('TWD Curncy', 1., 1),
    'NTN+1MUSD': CurrencyPair('NTN+1M Curncy', 1., 1),

    'CNYHKD': CurrencyPair('CNYHKD Curncy', 1., -1),
    'CNHHKD': CurrencyPair('CNHHKD Curncy', 1., -1),

    'HKDUSD': CurrencyPair('HKD Curncy', 1., 1),

    'EURUSD': CurrencyPair('EUR Curncy', 1., -1),

    'GBPUSD': CurrencyPair('GBP Curncy', 1., -1),
    'GBpUSD': CurrencyPair('GBP Curncy', 100., -1),
    'GBPAUD': CurrencyPair('GBPAUD Curncy', 1., -1),
    'GBpAUD': CurrencyPair('GBPAUD Curncy', 100., -1),
    'GBPEUR': CurrencyPair('GBPEUR Curncy', 1., -1),
    'GBpEUR': CurrencyPair('GBPEUR Curncy', 100., -1),
    'GBPHKD': CurrencyPair('GBPHKD Curncy', 1., -1),
    'GBpHKD': CurrencyPair('GBPHKD Curncy', 100., -1),

    'INT1USD': CurrencyPair('INT1 Curncy', 1., 1),
    'INT2USD': CurrencyPair('INT2 Curncy', 1., 1),

    'IRD1USD': CurrencyPair('IRD1 Curncy', 10000., -1),
    'IRD2USD': CurrencyPair('IRD2 Curncy', 10000., -1),

    'XID1USD': CurrencyPair('XID1 Curncy', 10000., -1),
    'XID2USD': CurrencyPair('XID2 Curncy', 10000., -1),
}
