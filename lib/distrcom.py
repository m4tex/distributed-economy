import asyncio
import threading

import grpc
from google.protobuf import empty_pb2
from protos import distrcom_rpcs_pb2
from protos import distrcom_rpcs_pb2_grpc

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

server = None

# Turns AIO rpc calls into tasks
def rpc_task(call):
    async def runner():
        return await call

    return asyncio.create_task(runner())

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
            print(f"[{self.consumer.time}] Join request received from consumer {peer_listen_ip}.")
            stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.consumers[request.assignedId] = (stub, peer_listen_ip)
        elif request.nodeType == distrcom_rpcs_pb2.NodeType.SHARED_RESOURCE:
            print(f"[{self.consumer.time}] Join request received from SR {peer_listen_ip}.")
            stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.sharedResources[peer_listen_ip] = stub

        return distrcom_rpcs_pb2.Timestamp(time=self.consumer.time)

    async def RespondDeferred(self, request, context):
        if request.nodeId not in self.consumer.awaiting_responses:
            raise Exception('Received unknown deferred response')

        if request.response == distrcom_rpcs_pb2.DEFERRED:
            raise Exception('Deferred response further deferred')

        print(f'Received deferred response from {request.nodeId}.')

        self.consumer.time = int(max(self.consumer.time, request.timestamp)) + 1

        self.consumer.awaiting_responses.remove(request.nodeId)

        if len(self.consumer.awaiting_responses) == 0:
            self.consumer.sr_access_event.set()

        return empty_pb2.Empty()

    async def RequestResource(self, request, context):
        # Local request, if the node is waiting on grants for a resource this is populated with (sr_address, timestamp)
        lr = self.consumer.ongoing_request

        self.consumer.time = int(max(self.consumer.time, request.timestamp)) + 1

        # First checks if the received request regards an SR that the local node also wants to access
        if lr is not None and lr[0] == request.srAddress:
            # If the local request comes before the received one time-wise
            local_timestamp_comes_first_condition = lr[1] < request.timestamp
            # If the timestamps are the same this condition is true if the local node has the priority by node id
            local_id_comes_first_condition = lr[1] == request.timestamp and self.consumer.nodeId < request.nodeId

            if local_timestamp_comes_first_condition or local_id_comes_first_condition:
                # Place the received request in the deferred queue
                self.consumer.deferred_responses.append(request.nodeId)

                print(f'[{self.consumer.time+1}] Received a conflicting request (node {request.nodeId}, [{request.timestamp}]), though I came first. Deferred.')

                return distrcom_rpcs_pb2.ResourceRequestReply(
                    nodeId=self.consumer.nodeId,
                    response=distrcom_rpcs_pb2.DEFERRED,
                    timestamp=self.consumer.time
                )

        print(f'[{self.consumer.time+1}] Received a request (node {request.nodeId}, [{request.timestamp}]). Sending OK.')

        return distrcom_rpcs_pb2.ResourceRequestReply(nodeId=self.consumer.nodeId, response=distrcom_rpcs_pb2.OK)

class SharedResourceConsumer:
    def __init__(self, port):
        self.port = port
        self.nodeId = -1
        # Lamport's clock time variable
        self.time = 0
        # Stores all connections, node id the key, (grpc stub, listen address) the value
        self.consumers = dict()
        # Active SR nodes
        self.sharedResources = dict()

        # Responses the consumer is waiting on before accessing SR
        self.awaiting_responses = list()

        # Keeps track of the node's request until free, format: (sr_address, timestamp)
        self.ongoing_request = None

        # Responses to be sent once the conflict is resolved, FIFO
        self.deferred_responses = list()

        self.sr_access_event = asyncio.Event()

    # Initializes the SR consumer. Runs cb_on_joined once successful.
    # Non-blocking, starts a listening server on a separate thread.
    def start(self, root_host, cb_on_joined):
        async def serve():
            global server

            if server is not None:
                raise Exception("Start called twice")

            server = grpc.aio.server()

            distrcom_rpcs_pb2_grpc.add_SRConsumerServicer_to_server(
                _SRConsumerHandler(self),
                server
            )

            server.add_insecure_port('0.0.0.0:{}'.format(self.port))
            await server.start()
            print(f'[{self.time}] SRConsumer started.')

            temp_stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(
                grpc.aio.insecure_channel(root_host)
            )

            join_response = await temp_stub.Join(
                distrcom_rpcs_pb2.NetworkJoinRequest(
                    listeningPort=self.port,
                    nodeType=distrcom_rpcs_pb2.CONSUMER
                )
            )

            self.consumers = {
                consumer_id: (
                    distrcom_rpcs_pb2_grpc.SRConsumerStub(
                        grpc.aio.insecure_channel(consumer_host)
                    ),
                    consumer_host
                ) for consumer_host, consumer_id in join_response.consumers.items()
            }

            self.sharedResources = {
                sr_host: distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(sr_host))
                for sr_host in join_response.shared_resources
            }

            self.sharedResources[root_host] = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(root_host))

            # TODO: add timeout

            join_responses = await asyncio.gather(*[stub.Join(
                distrcom_rpcs_pb2.NetworkJoinRequest(
                    listeningPort=self.port,
                    nodeType=distrcom_rpcs_pb2.CONSUMER,
                    assignedId=join_response.assignedId)
            ) for stub, _ in self.consumers.values()])

            # Sync time
            self.time = max([r.time for r in join_responses], default=0) + 1
            self.nodeId = join_response.assignedId

            print(f'[{self.time}] Join approved by all {len(join_responses)} consumers.')
            print(f'Current Consumers: {[v[1] for v in self.consumers.values()]}')
            print(f'Current shared resources: {list(self.sharedResources.keys())}')

            await cb_on_joined()
            await server.wait_for_termination()

        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()

    # Either selects an available SR or picks one at random to spread the workload
    def select_sr(self):
        pass

    # TODO: wrap callback in lock, free
    # Sends a request to the network, then calls the callback once access has been granted
    async def request(self, sr_addr):
        self.time += 1 # We send a request, therefore step time forward

        self.ongoing_request = (sr_addr, self.time)

        if len(self.consumers) == 0:
            print(f'[{self.time}] No one to send a request to. Access granted by default.')
            await self.acquire()
            self.time += 1
            return

        print(f'[{self.time}] Sending a request for {sr_addr}')
        request_payload = distrcom_rpcs_pb2.ResourceRequest(nodeId=self.nodeId, srAddress=sr_addr, timestamp=self.time)
        tasks = [
            rpc_task(stub.RequestResource(request_payload))
            for stub, _ in self.consumers.values()
        ]

        done, pending = await asyncio.wait(
            tasks,
            timeout=LIVENESS_TIMEOUT
        )

        if len(pending) > 0:
            pass # TODO: Handle stale nodes

        responses = [r.result() for r in done]

        # We receive responses, sync clock and increment
        self.time = max((self.time, *(r.timestamp for r in responses))) + 1

        # Save up the deferred responses
        deferred = [r for r in responses if r.response == distrcom_rpcs_pb2.DEFERRED]
        for d in deferred :
            self.awaiting_responses.append(d.nodeId)

        print(f'[{self.time}] Got deferred by: {[d.nodeId for d in deferred]}')

        # If there are conflicts, await access
        if len(self.awaiting_responses) > 0:
            await self.sr_access_event.wait()

        await self.acquire()

    # Wraps the consumer's critical section code in mutex-like lock and unlock for extra protection
    async def acquire(self):
        if self.ongoing_request is None:
            raise Exception("Freeing without ongoing request")

        sr_stub = self.sharedResources[self.ongoing_request[0]]

        try:
            acq_response = await asyncio.wait_for(
                sr_stub.Acquire(distrcom_rpcs_pb2.NodeId(id=self.nodeId)),
                timeout=LIVENESS_TIMEOUT
            )
        except asyncio.TimeoutError:
            # TODO: handle timeout (prolly retry or shutdown)
            raise Exception("SR acquire timed out")

        if not acq_response.OK:
            # This means the SR is currently taken
            raise Exception("SR acquire failed")

        print(f'[{self.time}] Access granted, SR acquired.')

    async def free(self):
        if self.ongoing_request is None:
            raise Exception("Freeing without ongoing request")

        sr_stub = self.sharedResources[self.ongoing_request[0]]

        try:
            free_response = await asyncio.wait_for(
                sr_stub.Free(distrcom_rpcs_pb2.NodeId(id=self.nodeId)),
                timeout=LIVENESS_TIMEOUT
            )
        except asyncio.TimeoutError:
            # TODO: handle timeout (prolly retry or shutdown)
            raise Exception("SR Free timed out")

        if not free_response.OK:
            # This means the SR has a different owner
            raise Exception("SR Free failed")

        self.ongoing_request = None
        self.time += 1

        if len(self.deferred_responses) == 0:
            print(f"Done with SR. Freed.")
            return

        tasks = [
            rpc_task(self.consumers[c][0].RespondDeferred(distrcom_rpcs_pb2.ResourceRequestReply(
                nodeId=self.nodeId,
                response=distrcom_rpcs_pb2.ResourceResponse.OK,
                timestamp=self.time
            )))
            for c in self.deferred_responses
        ]

        done, pending = await asyncio.wait(tasks, timeout=LIVENESS_TIMEOUT)

        print(f'[{self.time}] Done with SR. Sent deferred to: {self.deferred_responses}')

        if len(pending) > 0:
            pass # TODO: handle stale nodes

    async def operation(self):
        pass

# TODO: add lock timeout
class _SRProviderHandler(distrcom_rpcs_pb2_grpc.SharedResourceServicer):
    def __init__(self, provider):
        self.provider = provider

    async def Join(self, request, context):
        peer_listen_ip = f'{peer_ipv4(context)}:{request.listeningPort}'

        # Consumers excluding the node joining
        consumers_exclusive = self.provider.consumers.copy()

        if request.nodeType == distrcom_rpcs_pb2.SHARED_RESOURCE:
            print(f"Join request received from SR {peer_listen_ip}")
            self.provider.sharedResources.append(peer_listen_ip)
        elif request.nodeType == distrcom_rpcs_pb2.CONSUMER:
            print(f"Join request received from consumer {peer_listen_ip}")
            self.provider.nodeIdCounter += 1
            self.provider.consumers[peer_listen_ip] = self.provider.nodeIdCounter

        return distrcom_rpcs_pb2.NetworkJoinResponse(
            assignedId=self.provider.nodeIdCounter,
            consumers=consumers_exclusive,
            shared_resources=self.provider.sharedResources
        )

    async def IsLocked(self, request, context):
        return distrcom_rpcs_pb2.BooleanResponse(OK=(self.provider.currentOwner is not None))

    async def Free(self, request, context):
        if self.provider.currentOwner != request.id:
            print(f'ERROR: Free received from {request.id}, currently owned by {self.provider.currentOwner}')
            return distrcom_rpcs_pb2.BooleanResponse(OK=False)

        self.provider.currentOwner = None

        print(f'Free received from {request.id}')

        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def Acquire(self, request, context):
        if self.provider.currentOwner is not None:
            print(f'ERROR: Acquire received from {request.id}, owned by {self.provider.currentOwner}')
            return distrcom_rpcs_pb2.BooleanResponse(OK=False)

        print(f'Acquire received from {request.id}')

        self.provider.currentOwner = request.id
        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def ResourceOperation(self, request, context):
        if (request.nodeId != self.provider.currentOwner
                or request.operation not in self.provider.operationCallbacks):
            return distrcom_rpcs_pb2.SROperationReturn(OK=False)

        print(f'({request.nodeId}): Executing {request.operation}.')

        if request.HasField('args'):
            operation_return = self.provider.operationCallbacks[request.operation](self.provider.srData, request.args)
        else:
            operation_return = self.provider.operationCallbacks[request.operation](self.provider.srData)

        if operation_return is None:
            return distrcom_rpcs_pb2.SROperationReturn(OK=True)
        else:
            return distrcom_rpcs_pb2.SROperationReturn(OK=True, data=operation_return)


async def host_available(address, timeout=ROOT_DISCOVERY_TIMEOUT):
    channel = grpc.aio.insecure_channel(address)

    try:
        await asyncio.wait_for(
            channel.channel_ready(),
            timeout=timeout
        )
        return channel
    except TimeoutError:
        await channel.close()
        return False

class SharedResourceProvider:
    # Will be the ID of the node currently owning/locking the SR
    currentOwner = None
    consumers = dict()
    sharedResources = list()

    srData = None
    operationCallbacks = dict()
    nodeIdCounter = 0

    def __init__(self, initial_sr_data):
        self.srData = initial_sr_data

    # Initializes SR and starts the server thread. Non-blocking.
    def start(self, port = SERVER_PORT):
        async def serve():
            global server
            if server is not None:
                raise Exception("Start called twice")

            server = grpc.aio.server()

            distrcom_rpcs_pb2_grpc.add_SharedResourceServicer_to_server(_SRProviderHandler(self), server)

            server.add_insecure_port('0.0.0.0:{}'.format(port))

            await server.start()
            print('Shared resource provider started.')

            # TODO closer inspection once we run multiple SRs
            if f'{SERVER_IP}:{port}' != SERVER_HOST  and await host_available(SERVER_HOST):
                temp_stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(
                    grpc.aio.insecure_channel(SERVER_HOST)
                )

                join_response = await temp_stub.Join(
                    distrcom_rpcs_pb2.NetworkJoinRequest(listeningPort=port, nodeType=distrcom_rpcs_pb2.SHARED_RESOURCE)
                )

                self.consumers = dict(join_response.consumers)
                self.sharedResources = list(join_response.shared_resources)

                join_responses = list()
                try:
                    join_responses = await asyncio.wait_for(
                        asyncio.gather(*[distrcom_rpcs_pb2_grpc.SRConsumerStub(grpc.aio.insecure_channel(c)).Join(
                            distrcom_rpcs_pb2.NetworkJoinRequest(
                                listeningPort=port,
                                nodeType=distrcom_rpcs_pb2.SHARED_RESOURCE
                            )
                        ) for c, _ in self.consumers.items()]),
                        LIVENESS_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    print('Join timeout') # Maybe add handling later

                if join_response is None or not all([res.OK for res in join_responses]):
                    raise Exception("Join failed, response not OK")

                print(f'Join approved by all {len(join_responses)} consumers.')
            else:
                print('SR assumed root. (main network discovery node)')

            await server.wait_for_termination()

        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()

    # Adds an operation to the shared resource that acts on its stored data
    def add_resource_operation(self, name, callback):
        self.operationCallbacks[name] = callback