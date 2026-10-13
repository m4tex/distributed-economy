import asyncio
import os
import random
import threading
import time
import uuid

import grpc
from google.protobuf import empty_pb2
from protos import distrcom_rpcs_pb2 as pb
from protos import distrcom_rpcs_pb2_grpc as rpc

__all__ = ['SharedResourceConsumer', 'SharedResourceProvider']

SERVER_IP = '127.0.0.1'
SERVER_PORT = 8000
SERVER_HOST = f'{SERVER_IP}:{SERVER_PORT}'
ROOT_DISCOVERY_TIMEOUT = 0.5
LIVENESS_TIMEOUT = 1
RETRY_DELAY = 0.2
HEARTBEAT_INTERVAL = 1
REMOVAL_TIMEOUT = 8

# The probability of a lost request OR reply is 5% per incoming RPC.
SIMULATE_FAILURES = os.environ.get('DISTRCOM_SIMULATE_FAILURES', '1') == '1'
SIMULATE_SR_FAILURES = os.environ.get('DISTRCOM_SIMULATE_SR_FAILURES', '0') == '1'
MESSAGE_FAILURE_CHANCE = 0.05
# Whole-node events are sampled once per interval, not once per message.
FAULT_INTERVAL = 30
SILENCE_CHANCE = 0.5
SILENCE_SECONDS = 3
SHUTDOWN_CHANCE = 0.02

TRANSIENT_ERRORS = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.CANCELLED,
}


def peer_ipv4(context):
    peer = context.peer()
    if not peer.startswith('ipv4:'):
        raise ValueError('Only IPv4 is supported')
    return peer.split(':')[1]


class NetworkSessionLost(RuntimeError):
    """The SR restarted or removed this incarnation; old work must not replay."""


class _NetworkNode:
    def __init__(self):
        self._server = None
        self._channels = {}
        self._silent_until = 0
        self._next_fault = time.monotonic() + FAULT_INTERVAL
        self._random = random.Random()
        self._simulate_node_failures = True

    def _channel(self, address):
        if address not in self._channels:
            self._channels[address] = grpc.aio.insecure_channel(address)
        return self._channels[address]

    async def _network_ready(self):
        now = time.monotonic()
        if SIMULATE_FAILURES and self._simulate_node_failures and now >= self._next_fault:
            self._next_fault = now + FAULT_INTERVAL
            if self._random.random() < SHUTDOWN_CHANCE:
                print('Simulated node shutdown.', flush=True)
                os._exit(1)
            if self._random.random() < SILENCE_CHANCE:
                self._silent_until = now + SILENCE_SECONDS
                print(f'Simulated silence for {SILENCE_SECONDS} seconds.', flush=True)
        # Used on both inbound and outbound traffic: a silent node cannot
        # continue sending requests while merely refusing incoming messages.
        while time.monotonic() < self._silent_until:
            await asyncio.sleep(self._silent_until - time.monotonic())

    async def _call_once(self, method, payload):
        await self._network_ready()
        return await method(payload, timeout=LIVENESS_TIMEOUT, wait_for_ready=True)

    async def _call(self, method, payload):
        attempts = 0
        while True:
            try:
                return await self._call_once(method, payload)
            except grpc.aio.AioRpcError as error:
                if error.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise NetworkSessionLost(error.details()) from error
                if error.code() not in TRANSIENT_ERRORS:
                    raise
                attempts += 1
                if attempts == 1 or attempts % 10 == 0:
                    print(f'RPC unavailable; retrying (attempt {attempts}): {error.code().name}', flush=True)
                await asyncio.sleep(RETRY_DELAY + self._random.random() * RETRY_DELAY)

    async def _close(self):
        if self._server is not None:
            await self._server.stop(0)
        await asyncio.gather(*(channel.close() for channel in self._channels.values()))
        self._channels.clear()

    def _start_thread(self, coroutine):
        if getattr(self, '_thread', None) is not None:
            raise RuntimeError('Start called twice; create a new node to restart')

        def run():
            try:
                asyncio.run(coroutine())
            except Exception as error:
                # The example main thread waits forever. Fail visibly rather
                # than leave a dead server behind an apparently live process.
                print(f'Node stopped: {type(error).__name__}: {error}', flush=True)
                os._exit(1)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()


class NetworkInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, node):
        self.node = node

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        async def handle(request, context):
            await self.node._network_ready()
            failed = SIMULATE_FAILURES and self.node._random.random() < MESSAGE_FAILURE_CHANCE
            lose_reply = failed and self.node._random.random() < 0.5
            if failed and not lose_reply:
                await context.abort(grpc.StatusCode.UNAVAILABLE, 'Simulated lost request')
            # Ping/Join must work before startup completes. Gate inside the
            # handler, so waiting does not hold up interceptor dispatch.
            if handler_call_details.method in ('/SRConsumer/RequestResource', '/SRConsumer/RespondDeferred'):
                await self.node.ready.wait()
            response = await handler.unary_unary(request, context)
            if lose_reply:
                await context.abort(grpc.StatusCode.UNAVAILABLE, 'Simulated lost reply')
            return response

        return grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer)


class _SRConsumerHandler(rpc.SRConsumerServicer):
    def __init__(self, consumer):
        self.consumer = consumer

    async def Ping(self, request, context):
        # Before the root Join reply arrives, Ping is still proof of life.
        valid = not self.consumer.epoch or (
            request.epoch == self.consumer.epoch and request.id == self.consumer.nodeId)
        return pb.BooleanResponse(OK=valid)

    async def Join(self, request, context):
        self.consumer.time += 1
        if self.consumer.epoch and request.epoch != self.consumer.epoch:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Different SR incarnation')
        # Membership comes from the SR snapshot, never from an unconfirmed
        # peer announcement. Join remains a Lamport-clock synchronization RPC.
        return pb.Timestamp(time=self.consumer.time)

    async def RespondDeferred(self, request, context):
        consumer = self.consumer
        if (request.epoch == consumer.epoch and request.requestId == consumer._request_id
                and consumer.ongoing_request is not None and request.response == pb.OK):
            consumer.time = max(consumer.time, request.timestamp) + 1
            consumer.awaiting_responses.discard(request.nodeId)
            if not consumer.awaiting_responses:
                consumer.sr_access_event.set()
        # Delayed and duplicate grants are harmless, including after Free.
        return empty_pb2.Empty()

    async def RequestResource(self, request, context):
        consumer = self.consumer
        if request.epoch != consumer.epoch:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Different SR incarnation')
        consumer.time = max(consumer.time, request.timestamp) + 1
        previous = consumer._peer_requests.get(request.nodeId)
        if previous is not None and request.requestId < previous[0]:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Stale request')
        if previous is not None and request.requestId == previous[0]:
            response = previous[1]
        else:
            lr = consumer.ongoing_request
            conflict = lr is not None and lr[0] == request.srAddress
            priority = conflict and (consumer._holding or
                (lr[1], consumer.nodeId) < (request.timestamp, request.nodeId))
            response = pb.DEFERRED if priority else pb.OK
            consumer._peer_requests[request.nodeId] = (request.requestId, response)
            if priority:
                consumer.deferred_responses[request.nodeId] = request.requestId
        return pb.ResourceRequestReply(nodeId=consumer.nodeId, response=response,
            timestamp=consumer.time, requestId=request.requestId, epoch=consumer.epoch)


class SharedResourceConsumer(_NetworkNode):
    def __init__(self, port):
        super().__init__()
        self.port = port
        self.nodeId = -1
        self.time = 0
        self.consumers = {}
        self.sharedResources = {}
        self.awaiting_responses = set()
        self.ongoing_request = None
        self.deferred_responses = {}
        self.ready = asyncio.Event()
        self.sr_access_event = asyncio.Event()
        self.session = uuid.uuid4().hex
        self.epoch = ''
        self._revision = -1
        self._request_id = 0
        self._operation_id = 0
        self._peer_requests = {}
        self._holding = False
        self._root = None
        self._root_host = None
        self._api_lock = asyncio.Lock()

    def _identity(self, node_id=None):
        return pb.NodeId(id=self.nodeId if node_id is None else node_id,
            epoch=self.epoch, requestId=self._request_id)

    def _membership(self, response):
        if self.epoch and response.epoch != self.epoch:
            raise NetworkSessionLost('SR restarted; restart this consumer instead of replaying old operations')
        if self._revision > response.revision:
            return
        self.epoch = response.epoch
        self._revision = response.revision
        self.consumers = {
            node_id: (rpc.SRConsumerStub(self._channel(address)), address)
            for address, node_id in response.consumers.items() if node_id != self.nodeId
        }
        self.awaiting_responses.intersection_update(self.consumers)
        for node_id in list(self.deferred_responses):
            if node_id not in self.consumers:
                self.deferred_responses.pop(node_id, None)
        for node_id in list(self._peer_requests):
            if node_id not in self.consumers:
                self._peer_requests.pop(node_id, None)
        if not self.awaiting_responses:
            self.sr_access_event.set()

    async def _heartbeat(self):
        while True:
            response = await self._call(self._root.Heartbeat, self._identity())
            self._membership(response)
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _join(self, root_host):
        self._root_host = root_host
        self._root = rpc.SharedResourceStub(self._channel(root_host))
        response = await self._call(self._root.Join, pb.NetworkJoinRequest(
            listeningPort=self.port, nodeType=pb.CONSUMER, session=self.session))
        self.nodeId = response.assignedId
        self._membership(response)
        self.sharedResources[root_host] = self._root
        # Incoming peer requests can now be handled, even if a peer is silent
        # while we synchronize clocks. The SR lock remains the final guard.
        self.ready.set()
        for stub, _ in list(self.consumers.values()):
            try:
                reply = await self._call_once(stub.Join, pb.NetworkJoinRequest(
                    listeningPort=self.port, nodeType=pb.CONSUMER,
                    assignedId=self.nodeId, session=self.session, epoch=self.epoch))
                self.time = max(self.time, reply.time) + 1
            except grpc.aio.AioRpcError as error:
                if error.code() not in TRANSIENT_ERRORS:
                    raise
        print(f'[{self.time}][{self.nodeId}] Joined network with {len(self.consumers)} peers.', flush=True)

    async def _serve(self, root_host, cb_on_joined):
        background = []
        try:
            self._server = grpc.aio.server(interceptors=[NetworkInterceptor(self)],
                options=[('grpc.so_reuseport', 0)])
            rpc.add_SRConsumerServicer_to_server(_SRConsumerHandler(self), self._server)
            self.port = self._server.add_insecure_port(f'0.0.0.0:{self.port}')
            if not self.port:
                raise RuntimeError('Cannot bind consumer port')
            await self._server.start()
            await self._join(root_host)
            heartbeat = asyncio.create_task(self._heartbeat())
            callback = asyncio.create_task(cb_on_joined())
            background = [heartbeat, callback]
            done, _ = await asyncio.wait(background, return_when=asyncio.FIRST_COMPLETED)
            if heartbeat in done:
                await heartbeat
            if callback in done:
                await callback
                # A finite callback may finish while its server still serves peers.
                await heartbeat
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._close()

    def start(self, root_host, cb_on_joined):
        self._start_thread(lambda: self._serve(root_host, cb_on_joined))

    def select_sr(self):
        return next(iter(self.sharedResources), None)

    async def _ask_peer(self, node_id, payload):
        connection = self.consumers.get(node_id)
        if connection is None:
            return
        try:
            reply = await self._call_once(connection[0].RequestResource, payload)
            self.time = max(self.time, reply.timestamp) + 1
            if (reply.epoch == self.epoch and reply.requestId == self._request_id
                    and reply.response == pb.OK):
                self.awaiting_responses.discard(node_id)
        except grpc.aio.AioRpcError as error:
            if error.code() not in TRANSIENT_ERRORS:
                raise
            # A timeout is only suspicion. Only the SR may confirm removal.
            try:
                await self._call_once(self._root.Remove, self._identity(node_id))
            except grpc.aio.AioRpcError as removal_error:
                if removal_error.code() not in TRANSIENT_ERRORS:
                    raise

    async def request(self, sr_addr):
        async with self._api_lock:
            if self.ongoing_request is not None:
                raise RuntimeError('Request already in progress; free the previous resource first')
            if sr_addr not in self.sharedResources:
                raise ValueError('Unknown shared resource')
            self.time += 1
            self._request_id += 1
            self.ongoing_request = (sr_addr, self.time)
            self.sr_access_event.clear()
            self.awaiting_responses = set(self.consumers)
            payload = pb.ResourceRequest(nodeId=self.nodeId, srAddress=sr_addr,
                timestamp=self.time, requestId=self._request_id, epoch=self.epoch)
            while self.awaiting_responses:
                await asyncio.gather(*(self._ask_peer(node_id, payload)
                    for node_id in list(self.awaiting_responses)))
                if self.awaiting_responses:
                    # Poll the SAME request, so losing RespondDeferred cannot
                    # strand a waiter. Every attempt still has a 1s deadline.
                    await asyncio.sleep(RETRY_DELAY + self._random.random() * RETRY_DELAY)
            await self.acquire()

    async def acquire(self):
        if self.ongoing_request is None:
            raise RuntimeError('Acquiring without ongoing request')
        stub = self.sharedResources[self.ongoing_request[0]]
        while True:
            reply = await self._call(stub.Acquire, self._identity())
            if reply.OK:
                self._holding = True
                return
            await asyncio.sleep(RETRY_DELAY + self._random.random() * RETRY_DELAY)

    async def _send_deferred(self, node_id, request_id):
        connection = self.consumers.get(node_id)
        if connection is None:
            return
        try:
            await self._call_once(connection[0].RespondDeferred, pb.ResourceRequestReply(
                nodeId=self.nodeId, response=pb.OK, timestamp=self.time,
                requestId=request_id, epoch=self.epoch))
        except grpc.aio.AioRpcError as error:
            if error.code() not in TRANSIENT_ERRORS:
                raise
            # Its next RequestResource retry receives the cached OK.

    async def free(self):
        async with self._api_lock:
            if self.ongoing_request is None:
                raise RuntimeError('Freeing without ongoing request')
            stub = self.sharedResources[self.ongoing_request[0]]
            reply = await self._call(stub.Free, self._identity())
            if not reply.OK:
                raise NetworkSessionLost('SR rejected release')
            self.ongoing_request = None
            self._holding = False
            self.sr_access_event.clear()
            self.time += 1
            deferred = self.deferred_responses
            self.deferred_responses = {}
            for node_id, request_id in deferred.items():
                self._peer_requests[node_id] = (request_id, pb.OK)
            await asyncio.gather(*(self._send_deferred(node_id, request_id)
                for node_id, request_id in deferred.items()))

    async def operation(self, op_string, args=None):
        async with self._api_lock:
            if self.ongoing_request is None or not self._holding:
                raise RuntimeError('Operation without acquired resource')
            stub = self.sharedResources[self.ongoing_request[0]]
            self.time += 1
            self._operation_id += 1
            payload = pb.SROperation(nodeId=self.nodeId, operation=op_string,
                requestId=self._request_id, operationId=self._operation_id, epoch=self.epoch)
            if args is not None:
                payload.args = args
            reply = await self._call(stub.ResourceOperation, payload)
            if not reply.OK:
                raise RuntimeError(f'SR operation failed: {reply.error}')
            return reply.data if reply.HasField('data') else None


class _SRProviderHandler(rpc.SharedResourceServicer):
    def __init__(self, provider):
        self.provider = provider

    async def _member(self, node_id, epoch, context):
        if epoch != self.provider.epoch or node_id not in self.provider._members:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                'Node removed or SR restarted; restart the consumer with a new session')

    async def Join(self, request, context):
        provider = self.provider
        if request.nodeType != pb.CONSUMER:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, 'This failure protocol supports one SR')
        if not request.session:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Consumer session is required')
        address = f'{peer_ipv4(context)}:{request.listeningPort}'
        node_id = provider._sessions.get(request.session)
        if node_id is not None:
            if node_id not in provider._members:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Session already removed; restart consumer')
            if provider._members[node_id]['address'] != address:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Session changed address')
            return provider._snapshot(node_id)
        previous = provider.consumers.get(address)
        if previous is not None:
            provider._remove(previous)
        provider.nodeIdCounter += 1
        node_id = provider.nodeIdCounter
        provider._sessions[request.session] = node_id
        provider._members[node_id] = {
            'address': address, 'last_seen': time.monotonic(),
            'stub': rpc.SRConsumerStub(provider._channel(address)),
        }
        provider.consumers[address] = node_id
        provider._revision += 1
        print(f'Consumer {node_id} joined at {address}.', flush=True)
        return provider._snapshot(node_id)

    async def Heartbeat(self, request, context):
        await self._member(request.id, request.epoch, context)
        return self.provider._snapshot(request.id)

    async def Remove(self, request, context):
        if request.epoch != self.provider.epoch:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Different SR incarnation')
        member = self.provider._members.get(request.id)
        if member is not None and time.monotonic() - member['last_seen'] >= REMOVAL_TIMEOUT:
            self.provider._remove(request.id)
        return pb.BooleanResponse(OK=request.id not in self.provider._members)

    async def IsLocked(self, request, context):
        return pb.BooleanResponse(OK=self.provider.currentOwner is not None)

    async def Acquire(self, request, context):
        provider = self.provider
        await self._member(request.id, request.epoch, context)
        if request.requestId <= provider._released.get(request.id, 0):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Acquisition already released')
        if provider.currentOwner is not None:
            return pb.BooleanResponse(OK=(provider.currentOwner, provider._owner_request) ==
                (request.id, request.requestId))
        provider.currentOwner = request.id
        provider._owner_request = request.requestId
        return pb.BooleanResponse(OK=True)

    async def Free(self, request, context):
        provider = self.provider
        await self._member(request.id, request.epoch, context)
        if request.requestId <= provider._released.get(request.id, 0):
            return pb.BooleanResponse(OK=True)
        if (provider.currentOwner, provider._owner_request) != (request.id, request.requestId):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Release does not own resource')
        provider.currentOwner = None
        provider._owner_request = 0
        provider._released[request.id] = request.requestId
        return pb.BooleanResponse(OK=True)

    async def ResourceOperation(self, request, context):
        provider = self.provider
        await self._member(request.nodeId, request.epoch, context)
        previous = provider._operations.get(request.nodeId)
        encoded = request.SerializeToString()
        if previous is not None and request.operationId <= previous[0]:
            if request.operationId == previous[0] and encoded == previous[1]:
                return previous[2]
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Stale or changed operation ID')
        if request.operationId == 0 or (provider.currentOwner, provider._owner_request) != (
                request.nodeId, request.requestId):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Operation does not own resource')
        callback = provider.operationCallbacks.get(request.operation)
        if callback is None:
            response = pb.SROperationReturn(OK=False, error='Unknown operation')
        else:
            try:
                # Deliberately no await between mutation and recording result.
                # Callbacks must be short, synchronous operations on srData.
                data = callback(provider.srData, request.args) if request.HasField('args') else callback(provider.srData)
                response = pb.SROperationReturn(OK=True)
                if data is not None:
                    response.data = data
            except Exception as error:
                # A callback may have mutated data before raising. Cache its
                # failure too; never execute it again because a reply was lost.
                response = pb.SROperationReturn(OK=False, error=f'{type(error).__name__}: {error}')
        provider._operations[request.nodeId] = (request.operationId, encoded, response)
        return response


class SharedResourceProvider(_NetworkNode):
    def __init__(self, initial_sr_data):
        super().__init__()
        self._simulate_node_failures = SIMULATE_SR_FAILURES
        self.currentOwner = None
        self.consumers = {}
        self.sharedResources = []
        self.srData = initial_sr_data
        self.operationCallbacks = {}
        self.nodeIdCounter = 0
        self.epoch = uuid.uuid4().hex
        self._revision = 0
        self._members = {}
        self._sessions = {}
        self._released = {}
        self._operations = {}
        self._owner_request = 0

    def _snapshot(self, node_id):
        return pb.NetworkJoinResponse(assignedId=node_id,
            consumers={address: ident for address, ident in self.consumers.items() if ident != node_id},
            shared_resources=self.sharedResources, epoch=self.epoch, revision=self._revision)

    def _remove(self, node_id):
        member = self._members.pop(node_id, None)
        if member is None:
            return
        self.consumers.pop(member['address'], None)
        self._operations.pop(node_id, None)
        self._released.pop(node_id, None)
        if self.currentOwner == node_id:
            self.currentOwner = None
            self._owner_request = 0
        self._revision += 1
        # Session tombstones prevent a delayed Join from resurrecting an evictee.
        print(f'Removed silent consumer {node_id}; old requests are fenced out.', flush=True)

    async def _probe(self, node_id, member):
        try:
            reply = await self._call_once(member['stub'].Ping, pb.NodeId(id=node_id, epoch=self.epoch))
            if reply.OK and self._members.get(node_id) is member:
                member['last_seen'] = time.monotonic()
        except grpc.aio.AioRpcError as error:
            if error.code() not in TRANSIENT_ERRORS:
                print(f'Probe {node_id}: {error.code().name}', flush=True)
        # A probe for an old incarnation cannot remove a newer one.
        if (self._members.get(node_id) is member and
                time.monotonic() - member['last_seen'] >= REMOVAL_TIMEOUT):
            self._remove(node_id)

    async def _monitor(self):
        while True:
            await asyncio.gather(*(self._probe(node_id, member)
                for node_id, member in list(self._members.items())))
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _serve(self, port):
        try:
            self._server = grpc.aio.server(interceptors=[NetworkInterceptor(self)],
                options=[('grpc.so_reuseport', 0)])
            rpc.add_SharedResourceServicer_to_server(_SRProviderHandler(self), self._server)
            if not self._server.add_insecure_port(f'0.0.0.0:{port}'):
                raise RuntimeError('Cannot bind provider port')
            await self._server.start()
            print('Shared resource provider started.', flush=True)
            await self._monitor()
        finally:
            await self._close()

    def start(self, port=SERVER_PORT):
        self._start_thread(lambda: self._serve(port))

    def add_resource_operation(self, name, callback):
        self.operationCallbacks[name] = callback
