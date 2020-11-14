#!/usr/bin/env python
#
#  Paranoid Pirate worker
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
#   restart DEALER socket when queue restarts (timeout to receive queue heartbeat)
#   heartbeat to queue so queue unregisters this worker when unresponsive
#

import time
import sys
from random import randint
from datetime import datetime
import ctypes
import struct

from threading import Thread
from multiprocessing import Process

import zmq
from zmq import NOBLOCK, SNDMORE, Again

NOBLOCK_SNDMORE = NOBLOCK | SNDMORE

Container = Thread
identities = [b'A', b'B', b'C']

HEARTBEAT = b''
cycle = 0

port = sys.argv[1]
load_duration = float(sys.argv[2])

bind_endpoint = (f"tcp://*:{port}").encode()
connect_endpoint = (f"tcp://192.168.0.22:{port}").encode()


def p(msg):
    pass
    print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


def on_request(request, identity):
    address, control = request[:2]
    reply = [address, control]
    if control == HEARTBEAT:
        reply[1] = HEARTBEAT
        #p("I: RETURN PONG:  %s" % reply)
    else:
        reply.append(b"ACK" + control + b"-" + str(port).encode() + b"-" + identity.encode())
        # global cycle
        # cycle += 1
        # p("I: %s RETURN REPLY: %s, CYCLE=%d" % (identity, reply, cycle))
        # time.sleep(load_duration)  # Do some heavy work
    return reply


def worker_socket(worker_identity, context):
    worker = context.socket(zmq.REP)
    worker.connect("inproc://routing.ipc")
    worker.hwm = 100_000
    time.sleep(.2)  # Wait for threads to stabilize
    while 1:
        try:
            while 1:
                request = worker.recv_multipart(flags=zmq.NOBLOCK)
                # reply = on_request(request, worker_identity)
                worker.send_multipart(request, flags=zmq.NOBLOCK)
        except zmq.Again:
            time.sleep(.001)


def create_server_socket(context, bind, connect):
    server = context.socket(zmq.ROUTER)
    server.sndhwm = 1_000_000
    server.rcvhwm = 1_000_000
    server.identity = connect
    server.probe_router = 1
    server.router_mandatory = 1
    server.bind(bind)
    p("I: worker is ready at %s" % connect.decode())
    return server


context = zmq.Context(1)
server_socket = create_server_socket(context, bind_endpoint, connect_endpoint)

# router_socket = context.socket(zmq.DEALER)
# router_socket.bind("inproc://routing.ipc")
# for identity in identities:
#     Container(target=worker_socket, args=(identity.decode(), context)).start()
# time.sleep(1)  # Wait for threads to stabilize
# poller = zmq.Poller()
# poller.register(router_socket, zmq.POLLIN)
# max = randint(90, 500)

counter = tmp = 0
not_yet_started = True
start = 0
timeit = 0
request = None

# try:
#     while 1:
#         try:
#             while 1:
#                 # copy=True seems better for small messages
#                 request = server_socket.recv_multipart(flags=zmq.NOBLOCK, copy=True)
#                 if len(request) > 2:
#                     # print(request)
#                     counter += 1
#                     if not_yet_started:
#                         not_yet_started = False
#                         start = time.time()
#                     elif counter % 50_000 == 0:
#                         timeit = time.time()
#                     router_socket.send_multipart([b''] + request, flags=zmq.NOBLOCK)
#                 else:
#                     # return PONG
#                     server_socket.send_multipart(request, flags=zmq.NOBLOCK, copy=True)
#         except zmq.Again:
#             pass
#         try:
#             while 1:
#                 reply = router_socket.recv_multipart(flags=zmq.NOBLOCK)
#                 server_socket.send_multipart(reply[1:], flags=zmq.NOBLOCK)
#         except zmq.Again:
#             time.sleep(.002)
# except KeyboardInterrupt:
#     duration = timeit - start
#     print("%d requests,     %ds duration,    Average round trip cost: %d req/s " % (counter, duration, counter / duration))

# fmt = struct.Struct('I 9s')
# buffer = ctypes.create_string_buffer(fmt.size)
# fmt.pack_into(buffer, 0, )
# request_id, msg = fmt.unpack_from(request[1], 0)

while 1:
    try:
        while 1:
            hostname = server_socket.recv(flags=NOBLOCK_SNDMORE)
            request = server_socket.recv(flags=NOBLOCK)  # copy=True seems better for small messages
            if request:
                counter += 1
                if not_yet_started:
                    not_yet_started = False
                    start = time.time()
                elif counter % 100_000 == 0:
                    print("Average round trip cost: %d requests %d, req/s " % (counter, counter / (time.time() - start)))
            server_socket.send(hostname, flags=NOBLOCK_SNDMORE)
            server_socket.send(request, flags=NOBLOCK)

    except Again:
        hostname = server_socket.recv(SNDMORE)  # blocking call
        request = server_socket.recv(flags=NOBLOCK)
        if request:
            counter += 1
            if not_yet_started:
                not_yet_started = False
                start = time.time()
        server_socket.send(hostname, flags=NOBLOCK_SNDMORE)
        server_socket.send(request, flags=NOBLOCK)

# except KeyboardInterrupt:
#     duration = timeit - start
#     print("%d requests,     %ds duration,    Average round trip cost: %d req/s " % (counter, duration, counter / duration))


# while 1:
# try:
    # events = dict(poller.poll())

    # if events.get(server) == zmq.POLLIN:
    # p("I: RECEIVE REQUEST: %s" % request)
    # router.send_multipart([b''] + request)

    # if events.get(router) == zmq.POLLIN:
    #     reply = router.recv_multipart()
    #     server.send_multipart(reply[1:])
    #     p("I: RETURN REPLY: %s" % reply)

        # if cycle > max and port in ['5555','5556'] and randint(0, 50000000) == 0:
        #     p("I: Simulating CPU overload ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        #     time.sleep(randint(2, 6))
        # if cycle > max and randint(0, 5950000) == 0:
        #     p("I: Simulating a crash")
        #     import _thread
        #     _thread.interrupt_main()
        #     break

    # except:
    #     p("I: Interrupted!!!!!!!!!!")
    #     break
