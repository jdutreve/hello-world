import time
import asyncio
from datetime import datetime

import websockets

from websocket.rpc_agent import FreelanceClient
import rpc_agent

import uvloop
uvloop.install()

# TODO
# don't use asyncio for zmq : 5s
# fixer websockets est bloqué après full requester est fini
# https://kite.com/
# PING/PONG client
#           quid des PING queued/delayed (behind a lot of requests)?
#           client should squeeze too ancient returned PONGs
#               sending a lot PINGs to an expired server, will it reply a lot of (thousands) PONGS?
#           use tickless (finer grained heartbeat timeout)
#           different PING timeout per worker (depending on usage; ex: every 10ms or every 30s)
#  correct shutdown : LINGER sockopt, use disconnect?

REQUEST_NUMBER = 100_000

start = 0


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


async def send_requests():
    full_requester = client.create_requester()
    loop.create_task(read_replies(full_requester))
    global start
    start = time.time()

    for request_nb in range(REQUEST_NUMBER):
        await client.request(full_requester, [b"random name"])
        p("REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)


async def read_replies(full_requester):
    reply_nb = 0
    while True:
        reply = await client.receive(full_requester)
        reply_nb += 1
        if "FAILED" in reply[0].decode():
            p("FAIL %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
        else:
            pass
            p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (str(reply), reply_nb))
        if reply_nb == REQUEST_NUMBER:
            finish()
            loop.stop()
            break


def finish():
    duration = time.time() - start
    print("duration %s" % duration)
    print(
        "Rate: %d req/s ========================================================================================" %
        (REQUEST_NUMBER / duration))


async def init_websocket(websocket, path):
    try:
        requester = client.create_requester()
        loop.create_task(send_to_websocket(requester, websocket))
        while True:
            await client.request(requester, [b"AAAAAA"])
            await asyncio.sleep(.005)
    except Exception as e:
        print(e)
    finally:
        pass


async def send_to_websocket(requester, websocket):
    try:
        while True:
            reply = await client.receive(requester)
            if reply:
                await websocket.send(reply[1].decode())
            else:
                print("NULL")
    except Exception as e:
        print(e)
    finally:
        pass


def init_client():
    # await client.connect(b"tcp://127.0.0.1:5557")
    # await client.connect(b"tcp://127.0.0.1:5556")
    # await client.connect(b"tcp://127.0.0.1:5558")
    client.connect(b"tcp://192.168.0.22:5555")


loop = asyncio.get_event_loop()
client = FreelanceClient()


if __name__ == '__main__':
    init_client()
    loop.create_task(send_requests())
    # start_server = websockets.serve(init_websocket, "localhost", 5678)
    # loop.run_until_complete(start_server)
    loop.run_forever()
