import pynng
import trio
import fire
import orjson

from xbbg import blp
from functools import partial

DEFAULT_FDLS = [
    'MKTDATA_EVENT_TYPE', 'EVT_TRADE_DATE_RT', 'TIME',
    'TRADE_UPDATE_STAMP_RT', 'BID_UPDATE_STAMP_RT', 'ASK_UPDATE_STAMP_RT',
    'LAST_PRICE', 'RT_PX_CHG_PCT_1D', 'IS_DELAYED_STREAM',
    'VOLUME', 'EQY_TURNOVER_REALTIME',
]
ADDRESS = 'ipc:///xbbg/stream.ipc'


async def live(tickers, **kwargs):
    """
    Broadcasts live data feeds

    Args:
        tickers: list of tickers
        **kwargs: other parameters for `blp.live`
    """
    with pynng.Pub0() as pub:
        pub.listen(address=ADDRESS)
        async for data in blp.live(tickers=tickers, **kwargs):
            print(data)
            await pub.asend(orjson.dumps(data))


def main(**kwargs):

    kwargs['tickers'] = kwargs.get('tickers', ['ESA Index', 'CLA Comdty'])
    kwargs['info'] = kwargs.get('info', DEFAULT_FDLS)
    print(kwargs['tickers'])
    run_live = partial(live, **kwargs)
    trio.run(run_live)


if __name__ == "__main__":

    # Example: python pub.py --tickers="['SPY US Equity','XLE US Equity']"
    try:
        fire.Fire(main)
    except KeyboardInterrupt:
        pass
