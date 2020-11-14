#
#  Freelance Pattern
#
#   worker LRU load balancing (ROUTER + ready queue)
#   heartbeat to workers so they restart all (i.e. send READY) if queue is unresponsive
#

import time
import threading
import random
import asyncio

import zmq
import zmq.asyncio

from zhelpers import zpipe

# If no server replies after N retries, abandon request by returning FAILED!
REQUEST_RETRIES = 5

HEARTBEAT_INTERVAL = 500   # Milliseconds
HEARTBEAT_LIVENESS = 3      # 3..5 is reasonable
HEARTBEAT = b''

DEALER_QUEUE_SIZE = 100000   # Queue to access each WEBSOCKET
CLIENT_QUEUE_SIZE = 100000      # Queue to access the internal dispatcher
BACKEND_QUEUE_SIZE = 1000      # Queue to access external servers

too_late_nb = 0
reply_nb = 0


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


class FreelanceClient(object):
    context = None  # Our Context
    commands = None  # command socket in the client thread
    agent = None  # the background thread

    def __init__(self):
        context = zmq.Context()
        self.context = zmq.asyncio.Context()
        self.commands, agent_command_socket = zpipe(context, context, zmq.PAIR, zmq.PAIR)

        self.agent = threading.Thread(target=agent_task, args=(self.context, agent_command_socket))
        self.agent.daemon = False
        self.agent.start()

    def connect(self, endpoint):
        """Connect to new server endpoint
        Sends [CONNECT][endpoint] to the agent
        """
        self.commands.send_multipart([b"CONNECT", endpoint])

    def create_requester(self):
        requester = self.context.socket(zmq.DEALER)
        requester.sndhwm = DEALER_QUEUE_SIZE
        requester.connect("inproc://toto")
        return requester

    async def request(self, requester, msg):
        await requester.send_multipart([b"REQUEST"] + msg)

    async def receive(self, requester):
        return await requester.recv_multipart()


# =====================================================================
# Asynchronous part, works in the background thread


class Request(object):
    address = None  # zmq reply to address
    left_retries = 0
    msg = None      # Current request
    expires = 0     # Timeout for request/reply
    sequence = 0

    def __init__(self, sequence, msg, address):
        super().__init__()
        self.address = address
        self.msg = [str(sequence).encode()] + msg
        self.left_retries = REQUEST_RETRIES
        self.sequence = sequence
        result = self.compute_expires()
        self.expires = time.time() + result

    def retry(self, now):
        self.left_retries -= 1
        if self.left_retries < 1:
            return False
        result = self.compute_expires()
        self.expires = now + result
        return True

    def compute_expires(self):
        n = REQUEST_RETRIES - self.left_retries
        result = (3 ** n) * (random.random() + 1)
        #p("request timeout = %s" % result)
        return result


class FreelanceAgent(object):
    context = None  # Own context
    command_socket = None  # command Socket to talk back to client
    request_socket = None  # request Socket to talk back to client
    backend_socket = None  # Socket to talk to servers
    servers = None  # Servers we've connected to, used for sending PING
    actives = None  # Servers we know are alive (reply or PONG), used for fair load balancing
    sequence = 0  # Number of requests ever sent
    request = None  # Current request if any
    requests = None   # all pending requests

    def __init__(self, context, command_frontend):
        self.context = context
        self.command_socket = command_frontend

        self.request_socket = context.socket(zmq.ROUTER)
        self.request_socket.rcvhwm = CLIENT_QUEUE_SIZE
        self.request_socket.router_mandatory = 1
        self.request_socket.bind("inproc://toto")

        self.backend_socket = zmq.Context().socket(zmq.ROUTER)
        # make sure router doesn't drop unroutable message (host unreachable or again exception)
        self.backend_socket.router_mandatory = 1
        self.backend_socket.hwm = BACKEND_QUEUE_SIZE

        self.servers = {}
        self.actives = []
        self.request = None
        self.requests = {}
        self.start = time.time()

        asyncio.get_event_loop().create_task(self.on_command_message())

    async def on_command_message(self):
        msg = self.command_socket.recv_multipart()
        command = msg.pop(0)

        if command == b"CONNECT":
            endpoint = msg.pop(0)
            p("I: CONNECTING     %s" % [endpoint])
            self.backend_socket.connect(endpoint)
            server = Server(endpoint)
            self.servers[endpoint] = server
        else:
            p("E: Unknown command %s" % command)

    async def on_request_message(self):
        msg = await self.request_socket.recv_multipart()
        #p("DEALER RECEIVE %s" % msg)
        request = msg[1]

        if request == b"REQUEST":
            self.sequence += 1
            address = msg[0]
            self.request = Request(self.sequence, msg, address)
            self.requests[self.sequence] = self.request
        else:
            p("E: Unknown request %s" % request)

    async def on_reply_message(self):
        reply = self.backend_socket.recv_multipart()
        endpoint = reply[0]  # the server that replied
        server = self.servers[endpoint]
        server.reset_server_expiration()

        msg = reply[1:]
        if len(msg) == 1:
            if msg[0] is HEARTBEAT:
                p("I: RECEIVE PONG   %s" % [endpoint])
                server.connected = True
            else:
                p("E: Invalid message from Worker: %s" % reply)
        else:
            sequence = int(msg[0].decode())
            if sequence in self.requests:
                global reply_nb
                reply_nb += 1
                p("I: RECEIVE REPLY  %s : counter=%d" % (reply, reply_nb))
                request = self.requests.pop(sequence)
                msg = [request.address] + msg
                await self.request_socket.send_multipart(msg)
            else:
                global too_late_nb
                too_late_nb += 1
                #p("W: TOO LATE REPLY  %s" % reply)

        if not server.alive:
            server.alive = True
            p("I: SERVER ACTIVED %s-----------------------" % [server.address])

        # We want to move this responding server at the 'right place' in the actives queue, first remove it
        if server in self.actives:
            self.actives.remove(server)

        # Then, find the server having returned a reply the most recently (i.e. being truly alive)
        most_recently_received_index = 0
        for active in reversed(self.actives):  # reversed() because the most recent is at the end of the queue
            if active.is_last_operation_receive:
                most_recently_received_index = self.actives.index(active) + 1
                break

        # Finally, put the current server just behind the found server (Least Recently Used is the first in the queue)
        self.actives.insert(most_recently_received_index, server)
        server.is_last_operation_receive = True

    def send_request(self, server, request):
        request = [server.address] + request.msg
        self.backend_socket.send_multipart(request)
        p("I: SEND REQUEST   %s, ACTIVE: %s" % (request, self.actives))


def agent_task(ctx, command_socket):
    loop = asyncio.new_event_loop()
    loop.create_task(do_agent_task(ctx, command_socket))
    loop.run_forever()


async def do_agent_task(ctx, command_socket):

    agent = FreelanceAgent(ctx, command_socket)

    poll_backend = zmq.asyncio.Poller()
    poll_backend.register(agent.backend_socket, zmq.POLLIN)

    poll_all = zmq.asyncio.Poller()
    poll_all.register(agent.backend_socket, zmq.POLLIN)
    poll_all.register(agent.request_socket, zmq.POLLIN)

    while True:
        if len(agent.actives) > 0:
            events = dict(await poll_all.poll(HEARTBEAT_INTERVAL))
        else:
            events = dict(await poll_backend.poll(HEARTBEAT_INTERVAL))

        if events.get(agent.backend_socket) == zmq.POLLIN:
            await agent.on_reply_message()

        is_request_sent = False

        if events.get(agent.request_socket) == zmq.POLLIN:
            await agent.on_request_message()

            if agent.request and len(agent.actives) > 0:
                # Least recently used active server, i.e. queue head
                active_server = agent.actives[0]
                agent.send_request(active_server, agent.request)
                is_request_sent = True
                agent.request = None

        now = time.time()

        # Retry any expired requests
        if len(agent.requests) > 0 and len(agent.actives) > 0:
            active_server = agent.actives[0]
            for request in list(agent.requests.values()):
                if now >= request.expires:
                    if request.retry(now):
                        p("I: RETRYING REQUEST  %s, remaining %d" % (request.sequence, request.left_retries))
                        agent.send_request(active_server, request)
                        is_request_sent = True
                    else:
                        agent.requests.pop(request.sequence)
                        global reply_nb
                        reply_nb += 1
                        p("I: REQUEST FAILED  %d : counter=%d" % (request.sequence, reply_nb))
                        msg = [request.address, b"FAILED-"+str(request.sequence).encode()]
                        await agent.request_socket.send_multipart(msg)

        # Move the current active server at from the head to the end of the queue (Round-Robin)
        if is_request_sent:
            server = agent.actives.pop(0)
            agent.actives.append(server)
            server.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE
            server.ping_at = now + 1e-3 * HEARTBEAT_INTERVAL

        # Remove any expired servers
        for server in agent.actives[:]:
            if now >= server.expires:
                p("I: SERVER EXPIRED %s-----------------------" % [server.address])
                server.alive = False
                agent.actives.remove(server)

        # Send PING to idle servers if time has come
        for server in agent.servers.values():
            server.ping(agent.backend_socket, now)


class Server(object):
    address = None  # Server identity/address
    alive = False  # 1 if known to be alive
    connected = False
    ping_at = 0  # Next ping at this time
    expires = 0  # Expires at this time
    is_last_operation_receive = False  # Whether the last action for this server was a receive or send operation

    def __init__(self, address):
        self.address = address
        self.alive = False
        self.connected = False
        self.reset_server_expiration()
        self.is_last_operation_receive = False

    def reset_server_expiration(self):
        time_time = time.time()
        self.ping_at = time_time + 1e-3 * HEARTBEAT_INTERVAL
        self.expires = time_time + 1e-3 * HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def ping(self, backend_socket, now):
        if self.connected and self.alive and now > self.ping_at:
            p("I: SEND PING      %s" % [self.address])
            backend_socket.send_multipart([self.address, HEARTBEAT])
            self.ping_at = now + 1e-3 * HEARTBEAT_INTERVAL
            self.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE

    def tickless(self, tickless):
        if tickless > self.ping_at:
            tickless = self.ping_at
        return tickless

    def __repr__(self):
        return "%s-%s" % (self.address.decode().split(':')[2], self.is_last_operation_receive)
