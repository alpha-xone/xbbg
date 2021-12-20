import pynng
import trio
import fire

from functools import partial

ADDRESS = 'ipc:///xbbg/stream'


async def client(addr, max_msg=10):
    with pynng.Sub0() as sock:
        sock.subscribe('')
        list(map(sock.dial, [f'{ADDRESS}/{_}' for _ in addr]))
        while max_msg:
            msg = await sock.arecv_msg()
            print(msg.bytes)
            # print(orjson.loads(msg.bytes.decode()))
            max_msg -= 1


def main(**kwargs):

    run_client = partial(client, **kwargs)
    trio.run(run_client)


if __name__ == "__main__":

    # Example:
    #   python pub.py --channel=[futures,equity]
    try:
        fire.Fire(main)
    except KeyboardInterrupt:
        pass
