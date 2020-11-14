import time
from datetime import datetime
import asyncio

import zmq
import websockets

from rpc_agent import FreelanceClient

import uvloop
uvloop.install()

REQUEST_NUMBER = 10


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


async def init_websocket(websocket, path):
    requester = client.create_requester()
    loop.create_task(send_to_websocket_poller(requester, websocket))

    for request_nb in range(REQUEST_NUMBER):
        try:
            client.request(requester, [b"AAAAAA"])
            await websocket.send("REQUEST sent")
        except zmq.Again:
            print("QUEUE IS FULL REJECTING REQUEST +++++++++++++++++++++++++++++++++++")
        await asyncio.sleep(.05)


async def send_to_websocket_poller(requester, websocket):
    while True:
        try:
            reply = client.receive(requester)
            await websocket.send(reply[0].decode())
        except zmq.Again:
            pass
        await asyncio.sleep(.001)


def init_client():
    client.connect(b"tcp://192.168.0.22:5557")
    client.connect(b"tcp://192.168.0.22:5556")
    client.connect(b"tcp://192.168.0.22:5558")
    client.connect(b"tcp://192.168.0.22:5555")


client = FreelanceClient()
full_requester = client.create_requester()
loop = asyncio.get_event_loop()


if __name__ == '__main__':
    init_client()
    start_server = websockets.serve(init_websocket, "localhost", 5678)
    loop.run_until_complete(start_server)
    loop.run_forever()
