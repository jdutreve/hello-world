#
#  Freelance Pattern
#
#   worker LRU load balancing (ROUTER + ready queue)
#   heartbeat to workers so they restart all (i.e. send READY) if queue is unresponsive
#

import time
import threading
import random
import struct

import zmq
from zhelpers import zpipe

# If no server replies after N retries, abandon request by returning FAILED!
REQUEST_RETRIES = 5

HEARTBEAT = b''
HEARTBEAT_INTERVAL = 500    # Milliseconds
HEARTBEAT_INTERVAL_S = 1e-3 * HEARTBEAT_INTERVAL
HEARTBEAT_LIVENESS = 3      # 3..5 is reasonable
HEARTBEAT_LIVENESS_S = HEARTBEAT_INTERVAL_S * HEARTBEAT_LIVENESS

INBOUND_QUEUE_SIZE = 300_000      # Queue to respond to each client (websocket server, http server)
INTERNAL_QUEUE_SIZE = 300_000     # Queue to access the internal dispatcher
OUTBOUND_QUEUE_SIZE = 300_000     # Queue to call external servers

# Format for sending/receiving messages to/from external process (i.e tcp server)
#   - I (integer) for request.msg_id; Use L (long integer) instead if request.msg_id >= 4294967295
#   - H (short integer) for len(request.msg)
STRUCT_FORMAT = 'IH'
STRUCT_LENGTH = struct.calcsize(STRUCT_FORMAT)

retry_nb = 0
agent = None


def p(msg):
    pass
    # print('%s   %s' % (datetime.now().strftime('%M:%S:%f')[:-3], msg))


class FreelanceClient(object):
    """WARNING : A FreelanceClient instance is NOT thread safe, due to ZMQ socket being not thread safe

    The ZMQ Guide recommends creating a dedicated inproc socket for each thread (N DEALERS <--> 1 ROUTER).
    Or you may use a FreelanceClient instance in a single asyncio Event loop.
    """

    def __init__(self):
        self.context = zmq.Context()
        # command socket in the client thread
        self.command_socket, self.agent_command_socket = zpipe(self.context, self.context, zmq.PAIR, zmq.PAIR)

    def start(self):
        self.agent = threading.Thread(target=do_agent_task, args=(self.context, self.agent_command_socket))
        self.agent.start()
        self.request_socket = self._create_request_socket()

    def _create_request_socket(self):
        request_socket = self.context.socket(zmq.PAIR)
        request_socket.sndhwm = INBOUND_QUEUE_SIZE
        request_socket.rcvhwm = INBOUND_QUEUE_SIZE
        request_socket.connect("inproc://request.inproc")
        return request_socket

    def connect(self, endpoint):
        self.command_socket.send(endpoint.encode())

    def request(self, msg):
        self.request_socket.send(msg.encode(), flags=zmq.NOBLOCK)

    def receive(self):
        return self.request_socket.recv(flags=zmq.NOBLOCK).decode(), \
               self.request_socket.recv(flags=zmq.NOBLOCK).decode()


# =====================================================================
# Asynchronous part, works in the background thread


class FreelanceAgent(object):

    def __init__(self, context, command_frontend):
        self.context = context
        self.command_socket = command_frontend  # command Socket to talk back to client

        self.request_socket = context.socket(zmq.PAIR)  # request Socket to talk back to client
        self.request_socket.sndhwm = INTERNAL_QUEUE_SIZE
        self.request_socket.rcvhwm = INTERNAL_QUEUE_SIZE
        self.request_socket.bind("inproc://request.inproc")

        self.backend_socket = context.socket(zmq.ROUTER)
        self.backend_socket.router_mandatory = 1
        self.backend_socket.hwm = OUTBOUND_QUEUE_SIZE

        self.sequence = 0  # Number of requests ever sent
        self.reply_nb = 0
        self.failed_nb = 0
        self.servers = {}  # Servers we've connected to, and for sending PING
        self.actives = []  # Servers we know are alive (reply or PONG), used for fair load balancing
        self.request = None  # Current request if any
        self.requests = {}  # all pending requests
        self.latencies = []
        time.sleep(.1)

    def on_command_message(self):
        endpoint = self.command_socket.recv(flags=zmq.NOBLOCK)
        print("I: CONNECTING     %s" % endpoint)
        self.backend_socket.connect(endpoint.decode())
        self.servers[endpoint] = Server(endpoint)

    def on_request_message(self, now):
        msg = self.request_socket.recv(flags=zmq.NOBLOCK)
        self.sequence += 1
        self.requests[self.sequence] = request = Request(self.sequence, msg, now)
        self.send_request(request)

    def send_request(self, request):
        data = struct.pack(STRUCT_FORMAT, request.msg_id, len(request.msg)) + request.msg
        self.backend_socket.send(self.actives[0].address, flags=zmq.NOBLOCK|zmq.SNDMORE)
        self.backend_socket.send(data, flags=zmq.NOBLOCK)
        # p("I: SEND REQUEST   %s, ACTIVE!: %s" % (request, self.actives))

    def on_reply_message(self, now):
        # ex: reply = [b'tcp://192.168.0.22:5555', b'157REQ124'] or [b'tcp://192.168.0.22:5555', b'']
        server_hostname = self.backend_socket.recv(flags=zmq.NOBLOCK)
        server = self.servers[server_hostname]
        server.reset_server_expiration(now)
        server.is_last_operation_receive = True

        data = self.backend_socket.recv(flags=zmq.NOBLOCK)
        if data is HEARTBEAT:
            p("I: RECEIVE PONG   %s" % server_hostname)
            server.connected = True
        else:
            msg_id, msg_len = struct.unpack(STRUCT_FORMAT, data[:STRUCT_LENGTH])
            if msg_id in self.requests:
                self.send_reply(now, msg_id, data[-msg_len:], server_hostname)
            else:
                pass
                #p("W: TOO LATE REPLY  %s" % data[-msg_len:])

        if not server.alive:
            server.alive = True
            p("I: SERVER ALIVE %s-----------------------" % server.address)

        self.mark_as_active(server)

    def send_reply(self, now, msg_id, reply, server_name):
        request = self.requests.pop(msg_id)
        self.reply_nb += 1
        if server_name is None:
            p("W: REQUEST FAILED  %s" % msg_id)
        else:
            p("I: RECEIVE REPLY  %s" % server_name)
        self.request_socket.send(request.msg, flags=zmq.NOBLOCK|zmq.SNDMORE)
        self.request_socket.send(reply, flags=zmq.NOBLOCK)
        self.latencies.append(now - request.start)

    def mark_as_active(self, server):
        # We want to move this responding server at the 'right place' in the actives queue,

        # first remove it
        if server in self.actives:
            self.actives.remove(server)

        # Then, find the server having returned a reply the most recently (i.e. being truly alive)
        most_recently_received_index = 0
        for active in reversed(self.actives):  # reversed() because the most recent used is at the end of the queue
            if active.is_last_operation_receive:
                most_recently_received_index = self.actives.index(active) + 1
                break

        # Finally, put the given server just behind the found server (Least Recently Used is the first in the queue)
        self.actives.insert(most_recently_received_index, server)


def do_agent_task(ctx, command_socket):
    global agent
    agent = FreelanceAgent(ctx, command_socket)

    try:
        while True:
            agent.on_command_message()
    except zmq.Again:
        pass

    poll_backend = zmq.Poller()
    poll_backend.register(agent.backend_socket, zmq.POLLIN)

    poll_all = zmq.Poller()
    poll_all.register(agent.backend_socket, zmq.POLLIN)
    poll_all.register(agent.request_socket, zmq.POLLIN)

    while True:
        now = time.time()

        if len(agent.actives) > 0:
            events = dict(poll_all.poll(HEARTBEAT_INTERVAL))

            if events.get(agent.request_socket) == zmq.POLLIN:
                agent.on_request_message(now)
        else:
            events = dict(poll_backend.poll(HEARTBEAT_INTERVAL))

        if events.get(agent.backend_socket) == zmq.POLLIN:
            agent.on_reply_message(now)

        # is_request_sent = False

        # Retry any expired requests
        # if len(agent.requests) > 0 and len(agent.actives) > 0:
        #     for request in list(agent.requests.values()):
        #         if now >= request.expires:
        #             if request.retry(now):
        #                 print("I: RETRYING REQUEST  %s, remaining %d" % (request.msg_id, request.left_retries))
        #                 agent.send_request(request)
        #                 is_request_sent = True
        #                 global retry_nb
        #                 retry_nb += 1
        #             else:
        #                 agent.failed_nb += 1
        #                 agent.send_reply(now, request.msg_id, b"FAILED", None)

        # Move the current active server at from the head to the end of the queue (Round-Robin)
        # if is_request_sent and len(agent.actives) > 1:
        #     server = agent.actives.pop(0)
        #     agent.actives.append(server)
        #     server.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE
        #     server.ping_at = now + 1e-3 * HEARTBEAT_INTERVAL

        # Remove any expired servers
        # for server in agent.actives[:]:
        #     if now >= server.expires:
        #         p("I: SERVER EXPIRED %s-----------------------" % server.address)
        #         server.alive = False
        #         agent.actives.remove(server)

        # Send PING to idle servers if time has come
        # for server in agent.servers.values():
        #     server.ping(agent.backend_socket, now)


class Request(object):

    def __init__(self, msg_id, msg, now):
        self.msg_id = msg_id
        self.msg = msg
        self.left_retries = REQUEST_RETRIES
        self.expires = now + self._compute_expires()
        self.start = now

    def retry(self, now):
        self.left_retries -= 1
        if self.left_retries < 1:
            return False
        self.expires = now + self._compute_expires()
        return True

    def _compute_expires(self):
        n = REQUEST_RETRIES - self.left_retries
        result = 3 + (3 ** n) * (random.random() + 1)
        #p("request timeout = %s" % result)
        return result


class Server(object):

    def __init__(self, address):
        self.address = address  # Server identity/address
        self.alive = False  # 1 if known to be alive
        self.connected = False
        self.is_last_operation_receive = False  # Whether the last action for this server was a receive or send operation
        self.reset_server_expiration(time.time())

    def reset_server_expiration(self, now):
        self.ping_at = now + HEARTBEAT_INTERVAL_S  # Next ping at this time
        self.expires = now + HEARTBEAT_LIVENESS_S  # Expires at this time

    def ping(self, backend_socket, now):
        if self.connected and self.alive and now > self.ping_at:
            p("I: SEND PING      %s" % self.address)
            backend_socket.send(self.address, flags=zmq.NOBLOCK|zmq.SNDMORE)
            backend_socket.send(HEARTBEAT, flags=zmq.NOBLOCK)
            self.ping_at = now + HEARTBEAT_INTERVAL_S
            self.is_last_operation_receive = False  # last operation is now SEND, not RECEIVE

    def __repr__(self):
        return str(self.address)
