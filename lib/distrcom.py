import asyncio
import threading
import random
import time
import uuid

import grpc
from google.protobuf import empty_pb2
from protos import distrcom_rpcs_pb2, distrcom_rpcs_pb2_grpc

__all__ = [
    'SharedResourceConsumer',
    'SharedResourceProvider',
]

SERVER_IP = '127.0.0.1'
SERVER_PORT = 8000
SERVER_HOST = f'{SERVER_IP}:{SERVER_PORT}'

ROOT_DISCOVERY_TIMEOUT = 0.5

# To prevent 20 types of timeouts this will be the most generally used one
LIVENESS_TIMEOUT = 1

RETRY_ATTEMPTS = 5
MESSAGE_FAILURE_CHANCE = 0.05
SILENCE_CHANCE = 0.5
SHUTDOWN_CHANCE = 0.02
FAILURE_INTERVAL = 30
SILENCE_DURATION = 3

async def retry_rpc(method, payload, attempts=None, consumer=None):
    failures = 0
    while True:
        if consumer is not None:
            await consumer.wait_active()
        try:
            return await method(payload, timeout=LIVENESS_TIMEOUT)
        except grpc.aio.AioRpcError as error:
            if error.code() not in (grpc.StatusCode.UNAVAILABLE,
                                    grpc.StatusCode.DEADLINE_EXCEEDED,
                                    grpc.StatusCode.CANCELLED):
                raise
            failures += 1
            if attempts is not None and failures >= attempts:
                raise
            await asyncio.sleep(LIVENESS_TIMEOUT)

def peer_ipv4(context):
    peer = context.peer()
    if peer.startswith('ipv6'):
        raise Exception('ipv6 unhandled')

    return peer.split(':')[1]

# Handles RPC calls from deferred responses (part of SR conflict resolution)
class _SRConsumerHandler(distrcom_rpcs_pb2_grpc.SRConsumerServicer):
    def __init__(self, consumer):
        self.consumer = consumer

    async def Join(self, request, context):
        peer_listen_ip = f'{peer_ipv4(context)}:{request.listeningPort}'

        self.consumer.time += 1

        if request.nodeType == distrcom_rpcs_pb2.NodeType.CONSUMER and request.HasField('assignedId'):
            if request.assignedId in self.consumer.removed_nodes:
                return distrcom_rpcs_pb2.Timestamp(time=self.consumer.time)
            print(f"[{self.consumer.time}][{self.consumer.nodeId}] Join request received from consumer {peer_listen_ip}({request.assignedId}).")
            stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.consumers[request.assignedId] = (stub, peer_listen_ip)
        elif request.nodeType == distrcom_rpcs_pb2.NodeType.SHARED_RESOURCE:
            print(f"[{self.consumer.time}][{self.consumer.nodeId}] Join request received from SR {peer_listen_ip}.")
            stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.sharedResources[peer_listen_ip] = stub

        self.consumer.time += 1
        print(f'[{self.consumer.time}][{self.consumer.nodeId}] Approving join.')

        return distrcom_rpcs_pb2.Timestamp(time=self.consumer.time)

    async def Ping(self, request, context):
        return distrcom_rpcs_pb2.NodeId(id=max(0, self.consumer.nodeId))

    async def RespondDeferred(self, request, context):
        self.consumer.time = max(self.consumer.time, request.timestamp) + 1
        lr = self.consumer.ongoing_request
        # Duplicates and replies belonging to an earlier request are harmless.
        if (lr is not None and request.requestTimestamp == lr[1]
                and request.response == distrcom_rpcs_pb2.OK):
            self.consumer.awaiting_responses.discard(request.nodeId)
            if not self.consumer.awaiting_responses:
                self.consumer.sr_access_event.set()
        return empty_pb2.Empty()

    async def RequestResource(self, request, context):
        # Local request, if the node is waiting on grants for a resource this is populated with (sr_address, timestamp)
        lr = self.consumer.ongoing_request

        self.consumer.time = int(max(self.consumer.time, request.timestamp)) + 1

        print(f'[{self.consumer.time}][{self.consumer.nodeId}] Received a request (node {request.nodeId}, [{request.timestamp}]).')

        # First checks if the received request regards an SR that the local node also wants to access
        if lr is not None and lr[0] == request.srAddress:
            # If the local request comes before the received one time-wise
            local_timestamp_comes_first_condition = lr[1] < request.timestamp
            # If the timestamps are the same this condition is true if the local node has the priority by node id
            local_id_comes_first_condition = lr[1] == request.timestamp and self.consumer.nodeId < request.nodeId

            if local_timestamp_comes_first_condition or local_id_comes_first_condition:
                # Place the received request in the deferred queue
                self.consumer.deferred_responses[request.nodeId] = request.timestamp

                self.consumer.time += 1

                print(f'[{self.consumer.time}][{self.consumer.nodeId}] Request conflicting, I came first. Deferred.')

                return distrcom_rpcs_pb2.ResourceRequestReply(
                    nodeId=self.consumer.nodeId,
                    response=distrcom_rpcs_pb2.DEFERRED,
                    timestamp=self.consumer.time,
                    requestTimestamp=request.timestamp
                )

        self.consumer.time += 1

        print(f'[{self.consumer.time}][{self.consumer.nodeId}] Sending OK.')

        return distrcom_rpcs_pb2.ResourceRequestReply(
            nodeId=self.consumer.nodeId,
            response=distrcom_rpcs_pb2.OK,
            timestamp=self.consumer.time,
            requestTimestamp=request.timestamp
        )

class NetworkInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, consumer=None, label='SR'):
        self.consumer = consumer
        self.label = label

    def log_fault(self, message):
        node = self.label if self.consumer is None else (
            f'[{self.consumer.time}][{self.consumer.nodeId}]')
        print(f'[FAULT SIMULATION]{node} {message}', flush=True)

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        async def handle(request, context):
            consumer = self.consumer
            if consumer is not None:
                if consumer.stopped or time.monotonic() < consumer.silent_until:
                    self.log_fault(f'RPC REJECTED: {handler_call_details.method} from '
                                   f'{context.peer()} — node is shut down or temporarily silent.')
                    await context.abort(grpc.StatusCode.UNAVAILABLE, 'Node is silent')
                if handler_call_details.method not in ('/SRConsumer/Join', '/SRConsumer/Ping'):
                    await consumer.ready.wait()
            # A total 5% failure probability, split between request and reply loss.
            failure = random.random()
            if failure < MESSAGE_FAILURE_CHANCE / 2:
                self.log_fault(f'REQUEST DROPPED: {handler_call_details.method} from '
                               f'{context.peer()} — handler was not executed.')
                await context.abort(grpc.StatusCode.UNAVAILABLE, 'Simulated request loss')
            response = await handler.unary_unary(request, context)
            if failure < MESSAGE_FAILURE_CHANCE:
                self.log_fault(f'REPLY DROPPED: {handler_call_details.method} to '
                               f'{context.peer()} — handler completed, but its reply is lost.')
                await context.abort(grpc.StatusCode.UNAVAILABLE, 'Simulated reply loss')
            return response

        return grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer)

    async def simulate_failures(self):
        while True:
            await asyncio.sleep(FAILURE_INTERVAL)
            if random.random() < SHUTDOWN_CHANCE:
                self.log_fault('PERMANENT SHUTDOWN: stopping the gRPC server and consumer callback. '
                               'This node will not resume automatically.')
                self.consumer.stopped = True
                await self.consumer.server.stop(0)
                return
            if random.random() < SILENCE_CHANCE:
                self.log_fault(f'SILENCE START: {SILENCE_DURATION} seconds — incoming RPCs will be '
                               'rejected and outgoing library calls will wait.')
                self.consumer.silent_until = time.monotonic() + SILENCE_DURATION
                asyncio.get_running_loop().call_later(
                    SILENCE_DURATION, self.log_fault, 'SILENCE END: normal communication resumed.')

class SharedResourceConsumer:
    def __init__(self, port):
        self.port = port
        self.session = uuid.uuid4().hex
        self.server = None
        self.stopped = False
        self.silent_until = 0
        self.removed_nodes = set()
        self.nodeId = -1
        # Lamport's clock time variable
        self.time = 0
        # Stores all connections, node id the key, (grpc stub, listen address) the value
        self.consumers = dict()
        # Active SR nodes
        self.sharedResources = dict()

        # Responses the consumer is waiting on before accessing SR
        self.awaiting_responses = set()

        # Keeps track of the node's request until free, format: (sr_address, timestamp)
        self.ongoing_request = None

        # Responses to be sent once the conflict is resolved, keyed by node ID
        self.deferred_responses = dict()

        self.ready = asyncio.Event()
        self.sr_access_event = asyncio.Event()

    # Initializes the SR consumer. Runs cb_on_joined once successful.
    # Non-blocking, starts a listening server on a separate thread.
    def start(self, root_host, cb_on_joined):
        async def serve():
            if self.server is not None:
                raise Exception("Start called twice")

            interceptor = NetworkInterceptor(self)
            self.server = grpc.aio.server(interceptors=[interceptor])
            distrcom_rpcs_pb2_grpc.add_SRConsumerServicer_to_server(
                _SRConsumerHandler(self), self.server)
            self.port = self.server.add_insecure_port('0.0.0.0:{}'.format(self.port))
            await self.server.start()
            print(f'[{self.time}][{self.nodeId}] SRConsumer started.')

            temp_stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(
                grpc.aio.insecure_channel(root_host)
            )

            join_response = await retry_rpc(temp_stub.Join,
                distrcom_rpcs_pb2.NetworkJoinRequest(
                    listeningPort=self.port,
                    nodeType=distrcom_rpcs_pb2.CONSUMER,
                    session=self.session
                )
            )

            # Preserve peers announced by Join while the root RPC was in flight.
            self.consumers.update({
                consumer_id: (
                    distrcom_rpcs_pb2_grpc.SRConsumerStub(
                        grpc.aio.insecure_channel(consumer_host)
                    ),
                    consumer_host
                ) for consumer_host, consumer_id in join_response.consumers.items()
            })

            self.sharedResources.update({
                sr_host: distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(sr_host))
                for sr_host in join_response.shared_resources
            })

            self.sharedResources[root_host] = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(root_host))

            self.nodeId = join_response.assignedId
            join_payload = distrcom_rpcs_pb2.NetworkJoinRequest(
                listeningPort=self.port, nodeType=distrcom_rpcs_pb2.CONSUMER,
                assignedId=self.nodeId, session=self.session)
            # Register the same identity with any additional SRs.
            await asyncio.gather(*[retry_rpc(stub.Join, join_payload)
                                   for host, stub in self.sharedResources.items()
                                   if host != root_host])
            join_responses = await asyncio.gather(*[
                self.peer_rpc(node_id, 'Join', join_payload)
                for node_id in list(self.consumers)])
            self.time = max([self.time, *(r.time for r in join_responses if r is not None)]) + 1
            self.ready.set()
            simulation = asyncio.create_task(interceptor.simulate_failures())
            callback = asyncio.create_task(cb_on_joined())
            termination = asyncio.create_task(self.server.wait_for_termination())
            try:
                done, _ = await asyncio.wait([callback, termination],
                                             return_when=asyncio.FIRST_COMPLETED)
                if callback in done:
                    await callback
                    await termination
            finally:
                self.stopped = True
                for task in (simulation, callback, termination):
                    task.cancel()
                await asyncio.gather(simulation, callback, termination, return_exceptions=True)
                await self.server.stop(0)

        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()

    async def wait_active(self):
        if self.stopped:
            raise RuntimeError('Consumer has stopped; restart with a new instance')
        await asyncio.sleep(max(0, self.silent_until - time.monotonic()))
        if self.stopped:
            raise RuntimeError('Consumer has stopped; restart with a new instance')

    def remove_peer(self, node_id):
        self.removed_nodes.add(node_id)
        self.consumers.pop(node_id, None)
        self.deferred_responses.pop(node_id, None)
        self.awaiting_responses.discard(node_id)
        if not self.awaiting_responses:
            self.sr_access_event.set()

    async def peer_rpc(self, node_id, method, payload):
        while node_id in self.consumers:
            stub = self.consumers[node_id][0]
            try:
                response = await retry_rpc(getattr(stub, method), payload,
                                           RETRY_ATTEMPTS, self)
                if method != 'RequestResource' or response.nodeId == node_id:
                    return response
                # The address may now belong to a restarted node.
            except grpc.aio.AioRpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE,
                                        grpc.StatusCode.DEADLINE_EXCEEDED,
                                        grpc.StatusCode.CANCELLED):
                    raise
            # Never discard a peer just because one message was lost.
            removed = await asyncio.gather(*[
                retry_rpc(sr.RemoveNode, distrcom_rpcs_pb2.NodeId(id=node_id), consumer=self)
                for sr in self.sharedResources.values()])
            if all(result.OK for result in removed):
                self.remove_peer(node_id)
                return None
            await asyncio.sleep(LIVENESS_TIMEOUT)

    # Either selects an available SR or picks one at random to spread the workload
    def select_sr(self):
        pass

    async def request(self, sr_addr):
        await self.wait_active()
        if self.ongoing_request is not None:
            raise RuntimeError('A resource request is already active')
        if sr_addr not in self.sharedResources:
            raise ValueError('Unknown shared resource')
        self.time += 1
        self.ongoing_request = (sr_addr, self.time)
        self.sr_access_event.clear()
        self.awaiting_responses = set(self.consumers)
        payload = distrcom_rpcs_pb2.ResourceRequest(
            nodeId=self.nodeId, srAddress=sr_addr, timestamp=self.time)
        try:
            while self.awaiting_responses:
                responses = await asyncio.gather(*[
                    self.peer_rpc(node_id, 'RequestResource', payload)
                    for node_id in list(self.awaiting_responses)])
                for response in responses:
                    if response is not None:
                        self.time = max(self.time, response.timestamp) + 1
                        if response.response == distrcom_rpcs_pb2.OK:
                            self.awaiting_responses.discard(response.nodeId)
                if self.awaiting_responses:
                    try:
                        await asyncio.wait_for(self.sr_access_event.wait(), LIVENESS_TIMEOUT)
                    except asyncio.TimeoutError:
                        pass # Re-request also recovers a lost deferred response.
            await self.acquire()
        except BaseException:
            # Best effort cleanup on cancellation/error, without hiding the cause.
            try:
                await asyncio.wait_for(self.free(), LIVENESS_TIMEOUT)
            except (Exception, asyncio.CancelledError):
                # Stop serving so the SR can detect failure and reclaim ownership.
                self.stopped = True
                if self.server is not None:
                    await self.server.stop(0)
            raise

    # Wraps the consumer's critical section code in mutex-like lock and unlock for extra protection
    async def acquire(self):
        if self.ongoing_request is None:
            raise Exception("Acquiring without ongoing request")

        sr_stub = self.sharedResources[self.ongoing_request[0]]

        payload = distrcom_rpcs_pb2.NodeId(
            id=self.nodeId, requestTimestamp=self.ongoing_request[1])
        while True:
            response = await retry_rpc(sr_stub.Acquire, payload, consumer=self)
            if response.OK:
                print(f'[{self.time}][{self.nodeId}] Access granted, SR acquired.')
                return
            await asyncio.sleep(LIVENESS_TIMEOUT)

    async def free(self):
        if self.ongoing_request is None:
            raise Exception("Freeing without ongoing request")
        sr_stub = self.sharedResources[self.ongoing_request[0]]
        response = await retry_rpc(sr_stub.Free, distrcom_rpcs_pb2.NodeId(
            id=self.nodeId, requestTimestamp=self.ongoing_request[1]), consumer=self)
        if not response.OK:
            raise RuntimeError('SR Free rejected: this request is no longer the owner')
        self.ongoing_request = None
        self.awaiting_responses.clear()
        self.sr_access_event.clear()
        self.time += 1
        deferred = self.deferred_responses.copy()
        self.deferred_responses.clear()
        await asyncio.gather(*[
            self.peer_rpc(node_id, 'RespondDeferred', distrcom_rpcs_pb2.ResourceRequestReply(
                nodeId=self.nodeId, response=distrcom_rpcs_pb2.OK,
                timestamp=self.time, requestTimestamp=timestamp))
            for node_id, timestamp in deferred.items()])

    async def operation(self, op_string, args=None):
        if self.ongoing_request is None:
            raise Exception("Operation without ongoing request")
        sr_stub = self.sharedResources[self.ongoing_request[0]]
        self.time += 1
        payload = distrcom_rpcs_pb2.SROperation(
            nodeId=self.nodeId, operation=op_string, requestId=uuid.uuid4().hex,
            requestTimestamp=self.ongoing_request[1])
        if args is not None:
            payload.args = args
        response = await retry_rpc(sr_stub.ResourceOperation, payload, consumer=self)
        if not response.OK:
            raise RuntimeError('SR operation rejected: invalid operation or ownership')
        return response.data if response.HasField('data') else None


class _SRProviderHandler(distrcom_rpcs_pb2_grpc.SharedResourceServicer):
    def __init__(self, provider):
        self.provider = provider

    async def Join(self, request, context):
        host = f'{peer_ipv4(context)}:{request.listeningPort}'
        if request.nodeType == distrcom_rpcs_pb2.SHARED_RESOURCE:
            if host not in self.provider.sharedResources:
                self.provider.sharedResources.append(host)
            node_id = 0
        else:
            session = request.session or host
            node_id = self.provider.sessions.get(session)
            if node_id in self.provider.removed_nodes:
                await context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                                    'Removed session; restart with a new consumer instance')
            if node_id is None:
                old_id = self.provider.consumers.get(host)
                if old_id is not None and (not request.HasField('assignedId')
                                           or old_id != request.assignedId):
                    self.provider.remove_node(old_id)
                self.provider.nodeIdCounter += 1
                node_id = (request.assignedId if request.HasField('assignedId')
                           else self.provider.nodeIdCounter)
                if node_id in self.provider.removed_nodes:
                    await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Node was removed')
                self.provider.nodeIdCounter = max(self.provider.nodeIdCounter, node_id)
                self.provider.sessions[session] = node_id
                self.provider.consumers[host] = node_id
        return distrcom_rpcs_pb2.NetworkJoinResponse(
            assignedId=node_id,
            consumers={h: n for h, n in self.provider.consumers.items() if h != host},
            shared_resources=self.provider.sharedResources)

    async def RemoveNode(self, request, context):
        if request.id in self.provider.removed_nodes:
            return distrcom_rpcs_pb2.BooleanResponse(OK=True)
        # Detection runs independently of the one-second RPC deadline.
        self.provider.suspect_node(request.id)
        return distrcom_rpcs_pb2.BooleanResponse(OK=False)

    async def IsLocked(self, request, context):
        return distrcom_rpcs_pb2.BooleanResponse(OK=self.provider.currentOwner is not None)

    async def Free(self, request, context):
        if self.provider.last_free.get(request.id, -1) >= request.requestTimestamp:
            return distrcom_rpcs_pb2.BooleanResponse(OK=True)
        if self.provider.currentOwner == request.id:
            if self.provider.owner_timestamp != request.requestTimestamp:
                return distrcom_rpcs_pb2.BooleanResponse(OK=False)
            self.provider.currentOwner = None
            self.provider.operation_results.pop(request.id, None)
        # Remember even cancelled requests, fencing a late Acquire message.
        self.provider.last_free[request.id] = request.requestTimestamp
        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def Acquire(self, request, context):
        if request.id not in self.provider.consumers.values():
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Node is not registered')
        if self.provider.last_free.get(request.id, -1) >= request.requestTimestamp:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Request already released')
        if self.provider.currentOwner is not None:
            return distrcom_rpcs_pb2.BooleanResponse(
                OK=(self.provider.currentOwner == request.id
                    and self.provider.owner_timestamp == request.requestTimestamp))
        self.provider.currentOwner = request.id
        self.provider.owner_timestamp = request.requestTimestamp
        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def ResourceOperation(self, request, context):
        if (request.nodeId != self.provider.currentOwner
                or request.requestTimestamp != self.provider.owner_timestamp
                or request.operation not in self.provider.operationCallbacks):
            return distrcom_rpcs_pb2.SROperationReturn(OK=False)
        results = self.provider.operation_results.setdefault(request.nodeId, {})
        if request.requestId and request.requestId in results:
            response, error = results[request.requestId]
        else:
            try:
                callback = self.provider.operationCallbacks[request.operation]
                data = (callback(self.provider.srData, request.args) if request.HasField('args')
                        else callback(self.provider.srData))
                response = distrcom_rpcs_pb2.SROperationReturn(OK=True)
                if data is not None:
                    response.data = data
                error = None
            except Exception as exc:
                # A callback may have partially mutated data: never rerun it automatically.
                response, error = None, str(exc)
            if request.requestId:
                results[request.requestId] = (response, error)
        if error is not None:
            await context.abort(grpc.StatusCode.INTERNAL, error)
        return response


async def host_available(address, timeout=ROOT_DISCOVERY_TIMEOUT):
    channel = grpc.aio.insecure_channel(address)

    try:
        await asyncio.wait_for(
            channel.channel_ready(),
            timeout=timeout
        )
        return True
    except TimeoutError:
        return False
    finally:
        await channel.close()

class SharedResourceProvider:
    def __init__(self, initial_sr_data):
        self.srData = initial_sr_data
        self.currentOwner = None
        self.owner_timestamp = None
        self.consumers = dict()
        self.sharedResources = list()
        self.operationCallbacks = dict()
        self.nodeIdCounter = 0
        self.sessions = dict()
        self.removed_nodes = set()
        self.last_free = dict()
        self.operation_results = dict()
        self.checks = dict()
        self.server = None

    def remove_node(self, node_id):
        self.removed_nodes.add(node_id)
        self.consumers = {h: n for h, n in self.consumers.items() if n != node_id}
        if self.currentOwner == node_id:
            self.currentOwner = None
        self.operation_results.pop(node_id, None)
        self.last_free.pop(node_id, None)
        print(f'Removed consumer {node_id}.')

    def suspect_node(self, node_id):
        if node_id in self.checks:
            return
        async def check():
            try:
                host = next((h for h, n in self.consumers.items() if n == node_id), None)
                if host is not None:
                    async with grpc.aio.insecure_channel(host) as channel:
                        stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(channel)
                        try:
                            response = await retry_rpc(stub.Ping, empty_pb2.Empty(), RETRY_ATTEMPTS)
                            if response.id in (0, node_id):
                                return
                        except grpc.aio.AioRpcError:
                            pass
                self.remove_node(node_id)
            finally:
                self.checks.pop(node_id, None)
        self.checks[node_id] = asyncio.create_task(check())

    async def monitor(self):
        while True:
            for node_id in list(self.consumers.values()):
                self.suspect_node(node_id)
            await asyncio.sleep(LIVENESS_TIMEOUT)

    # Initializes SR and starts the server thread. Non-blocking.
    def start(self, port=SERVER_PORT):
        async def serve():
            if self.server is not None:
                raise Exception("Start called twice")
            self.server = grpc.aio.server(interceptors=[NetworkInterceptor(label=f'SR port {port}')])
            distrcom_rpcs_pb2_grpc.add_SharedResourceServicer_to_server(
                _SRProviderHandler(self), self.server)
            self.server.add_insecure_port('0.0.0.0:{}'.format(port))
            await self.server.start()
            print('Shared resource provider started.')
            if f'{SERVER_IP}:{port}' != SERVER_HOST and await host_available(SERVER_HOST):
                root = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(SERVER_HOST))
                response = await retry_rpc(root.Join, distrcom_rpcs_pb2.NetworkJoinRequest(
                    listeningPort=port, nodeType=distrcom_rpcs_pb2.SHARED_RESOURCE))
                self.consumers.update(response.consumers)
                self.sharedResources = list(response.shared_resources)
                async def announce(host, node_id):
                    async with grpc.aio.insecure_channel(host) as channel:
                        stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(channel)
                        while node_id not in self.removed_nodes:
                            try:
                                await retry_rpc(stub.Join, distrcom_rpcs_pb2.NetworkJoinRequest(
                                    listeningPort=port, nodeType=distrcom_rpcs_pb2.SHARED_RESOURCE),
                                    RETRY_ATTEMPTS)
                                return
                            except grpc.aio.AioRpcError as error:
                                if error.code() not in (grpc.StatusCode.UNAVAILABLE,
                                                        grpc.StatusCode.DEADLINE_EXCEEDED,
                                                        grpc.StatusCode.CANCELLED):
                                    raise
                                self.suspect_node(node_id)
                                await self.checks[node_id]
                await asyncio.gather(*[announce(host, node_id)
                                       for host, node_id in list(self.consumers.items())])
            monitor = asyncio.create_task(self.monitor())
            try:
                await self.server.wait_for_termination()
            finally:
                monitor.cancel()
                checks = list(self.checks.values())
                for task in checks:
                    task.cancel()
                await asyncio.gather(monitor, *checks, return_exceptions=True)
                await self.server.stop(0)
        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()

    # Adds an operation to the shared resource that acts on its stored data
    def add_resource_operation(self, name, callback):
        self.operationCallbacks[name] = callback
