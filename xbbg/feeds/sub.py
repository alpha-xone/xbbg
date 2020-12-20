import pynng
import orjson
import trio
import fire

from functools import partial

ADDRESS = 'ipc:///xbbg/stream.ipc'


async def client(max_msg=10):
    with pynng.Sub0() as sock:
        sock.subscribe('')
        sock.dial(ADDRESS)

        while max_msg:
            msg = await sock.arecv_msg()
            print(orjson.loads(msg.bytes.decode()))
            max_msg -= 1


def main(**kwargs):

    run_client = partial(client, **kwargs)
    trio.run(run_client)


if __name__ == "__main__":

    try:
        fire.Fire(main)
    except KeyboardInterrupt:
        pass
