import blpapi


RESPONSE_ERROR = blpapi.Name("responseError")
SESSION_TERMINATED = blpapi.Name("SessionTerminated")
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")

BAR_DATA = blpapi.Name('barData')
TICK_DATA = blpapi.Name('barTickData')

BAR_TITLE = 'getElementAsDatetime', blpapi.Name('time')
BAR_ITEMS = {
    'open': ('getElementAsFloat', blpapi.Name('open')),
    'high': ('getElementAsFloat', blpapi.Name('high')),
    'low': ('getElementAsFloat', blpapi.Name('low')),
    'close': ('getElementAsFloat', blpapi.Name('close')),
    'volume': ('getElementAsInteger', blpapi.Name('volume')),
    'num_trds': ('getElementAsInteger', blpapi.Name('numEvents')),
}
