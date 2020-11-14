import time
import asyncio
import statistics
from datetime import datetime
import zmq

from rpc_agent_ok import FreelanceClient
import rpc_agent_ok as rpc_agent

import uvloop
uvloop.install()

ZMQ_AGAIN = zmq.Again


# TODO
# 64 920 req/s with 6 clients 100 000
#
# GO implementation
# remove @TODO
# more fair load balancing
# fixer l'arrêt d'un seul server
# profile server
# PING/PONG client
#           quid des PING queued/delayed (behind a lot of requests)?
#           client should squeeze too ancient returned PONGs
#               sending a lot PINGs to an expired server, will it reply a lot of (thousands) PONGS?
#           use tickless (finer grained heartbeat timeout)
#           different PING timeout per worker (depending on usage; ex: every 10ms or every 30s)
#  correct shutdown : LINGER sockopt, use disconnect?

REQUEST_NUMBER = 100_000


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


async def send_requests_poller():
    for request_nb in range(1, REQUEST_NUMBER+1):
        try:
            client.request("REQ%d" % request_nb)
        except ZMQ_AGAIN:
            print("QUEUE IS FULL REJECTING REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)
        else:
            pass
            # p("REQUEST %d +++++++++++++++++++++++++++++++++++" % request_nb)
        await asyncio.sleep(0)
    print("SEND REQUESTS FINISHED")


async def read_replies_poller():
    reply_nb = 0
    while True:
        try:
            while True:
                request, reply = client.receive()
                reply_nb += 1
                p("REPLY %s %d +++++++++++++++++++++++++++++++++++" % (reply, reply_nb))
                if reply_nb == REQUEST_NUMBER:
                    print("**************************** READ REPLIES FINISHED ****************************")
                    return
        except ZMQ_AGAIN:
            await asyncio.sleep(.006)  # should not be MUCH smaller (else worst performance)


def finish(start):
    duration = time.time() - start
    print("duration %s" % duration)
    print("Rate: %d req/s ========================================================================================" % (REQUEST_NUMBER / duration))
    print("REPLY NB = %d" % rpc_agent.agent.reply_nb)
    print("FAILED NB = %d" % rpc_agent.agent.failed_nb)
    print("LATENCY p50 = %fms" % (statistics.median(rpc_agent.agent.latencies)*1000))
    print("LATENCY p90 = %fms" % ([round(q, 1000) for q in statistics.quantiles(rpc_agent.agent.latencies, n=10)][-1]*1000))
    # with open('latencies.csv', 'w') as f:
    #     for i in rpc_agent.agent.latencies:
    #         f.write("%s\n" % str(i))
    print("RETRY NB = %d" % rpc_agent.retry_nb)


def init_client(client):
    # client.connect("tcp://192.168.0.22:5557")
    # client.connect("tcp://192.168.0.22:5556")
    # client.connect("tcp://192.168.0.22:5558")
    client.connect("tcp://192.168.0.22:5555")


client = FreelanceClient()
init_client(client)
client.start()


async def main():
    # import yappi
    # yappi.set_clock_type("WALL")
    # yappi.start()

    start = time.time()

    task1 = asyncio.ensure_future(send_requests_poller())
    task2 = asyncio.ensure_future(read_replies_poller())
    await task1
    await task2

    finish(start)

    # yappi.get_func_stats().print_all(columns={0: ("name", 100), 1: ("ncall", 10), 2: ("tsub", 8), 3: ("ttot", 8), 4: ("tavg", 8)})
    # yappi.stop()


asyncio.run(main())
