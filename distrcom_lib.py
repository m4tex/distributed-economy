import asyncio
import threading

import grpc
from google.protobuf import empty_pb2
from protos import distrcom_rpcs_pb2, distrcom_rpcs_pb2_grpc

SERVER_IP = '127.0.0.1'
SERVER_PORT = 8000
SERVER_HOST = f'{SERVER_IP}:{SERVER_PORT}'

ROOT_DISCOVERY_TIMEOUT = 0.5
JOIN_TIMEOUT = 1

server = None

def peer_ipv4(context):
    peer = context.peer()
    if peer.startswith('ipv6'):
        raise Exception('ipv6 unhandled')

    return peer.split(':')[1]

# Handles RPC calls from deferred responses (part of SR conflict resolution)
class SRConsumerHandler(distrcom_rpcs_pb2_grpc.SRConsumerServicer):
    def __init__(self, consumer):
        self.consumer = consumer

    async def Join(self, request, context):
        peer_listen_ip = f'{peer_ipv4(context)}:{request.listeningPort}'

        if request.nodeType == distrcom_rpcs_pb2.NodeType.CONSUMER and request.HasField('assignedId'):
            print(f"Join request received from consumer {peer_listen_ip}.")
            stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.consumers[request.assignedId] = stub
        elif request.nodeType == distrcom_rpcs_pb2.NodeType.SHARED_RESOURCE:
            print(f"Join request received from SR {peer_listen_ip}.")
            stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(peer_listen_ip))
            self.consumer.sharedResources[peer_listen_ip] = stub

        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def RespondDeferred(self, request, context):
        # handle later
        print("Deferred response received")

        return empty_pb2.Empty()

    async def Request(self, resource, host):
        print("Request received")


class SRConsumer:
    def __init__(self, port):
        self.port = port
        # Lamport's clock time variable
        self.time = 0
        # Stores all connections, full ip string the key, (grpc stub, node id) the value
        self.consumers = dict()
        # Active SR nodes
        self.sharedResources = dict()

        # Responses the consumer is waiting on before accessing SR
        self.awaiting_responses = list()

        # Keeps track of the node's request (that's still to be resolved)
        self.ongoing_request_timestmap = None

        # Responses to be sent once the conflict is resolved, FIFO
        self.deferred_responses = list()

    def Start(self, root_host):
        async def serve():
            global server

            if server is not None:
                raise Exception("Start called twice")

            server = grpc.aio.server()

            distrcom_rpcs_pb2_grpc.add_SRConsumerServicer_to_server(
                SRConsumerHandler(self),
                server
            )

            server.add_insecure_port('0.0.0.0:{}'.format(self.port))
            await server.start()
            print('SRConsumer started.')

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
                consumer_id: distrcom_rpcs_pb2_grpc.SRConsumerStub(
                    grpc.aio.insecure_channel(consumer_host)
                ) for consumer_host, consumer_id in join_response.consumers.items()
            }

            self.sharedResources = {
                sr_host: distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(sr_host))
                for sr_host in join_response.shared_resources
            }

            self.sharedResources[root_host] = distrcom_rpcs_pb2_grpc.SharedResourceStub(grpc.aio.insecure_channel(root_host))

            join_responses = await asyncio.gather(*[stub.Join(
                distrcom_rpcs_pb2.NetworkJoinRequest(
                    listeningPort=self.port,
                    nodeType=distrcom_rpcs_pb2.CONSUMER,
                    assignedId=join_response.assignedId)
            ) for _, stub in self.consumers.items()])

            if not all([res.OK for res in join_responses]):
                raise Exception("Join failed, response not OK")

            print(f'Join approved by all {len(join_responses)} consumers.')
            print(f'Current Consumers: {self.consumers.keys()}')
            print(f'Current shared resources: {self.sharedResources.keys()}')

            await server.wait_for_termination()

        threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()

    # Either selects an available SR or picks one at random to spread the workload
    def SelectSR(self):
        pass

    def Request(self, sr_node):
        for stub, _ in self.consumers.values():
            stub.Request(sr_node)

# TODO add lock timeout, failure handling

class SRProviderHandler(distrcom_rpcs_pb2_grpc.SharedResourceServicer):
    def __init__(self, provider):
        self.provider = provider

    async def Join(self, request, context):
        peer = context.peer()
        if peer.startswith('ipv6'):
            raise Exception('ipv6 unhandled')

        peer_listen_ip = f'{peer_ipv4(context)}:{request.listeningPort}'

        consumers_exclusive = self.provider.consumers.copy()

        if request.nodeType == distrcom_rpcs_pb2.SHARED_RESOURCE:
            print(f"Join request received from SR {peer_listen_ip}")
            self.provider.sharedResources.append(peer_listen_ip)
        elif request.nodeType == distrcom_rpcs_pb2.CONSUMER:
            print(f"Join request received from consumer {peer_listen_ip}")
            self.provider.currentNodeId += 1
            self.provider.consumers[peer_listen_ip] = self.provider.currentNodeId

        return distrcom_rpcs_pb2.NetworkJoinResponse(
            assignedId=self.provider.currentNodeId,
            consumers=consumers_exclusive,
            shared_resources=self.provider.sharedResources
        )

    async def IsLocked(self, request, context):
        return distrcom_rpcs_pb2.BooleanResponse(OK=(self.provider.currentOwner is not None))

    async def Free(self, request, context):
        if self.provider.currentOwner != request.id:
            return distrcom_rpcs_pb2.BooleanResponse(OK=False)

        self.provider.currentOwner = None
        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def Acquire(self, request, context):
        if self.provider.currentOwner is not None:
            return distrcom_rpcs_pb2.BooleanResponse(OK=False)

        self.provider.currentOwner = request.id
        return distrcom_rpcs_pb2.BooleanResponse(OK=True)

    async def RequestResource(self, request, context):
        print("Resource request received")

        # handle later
        return distrcom_rpcs_pb2.ResourceRequestReply()

    async def ResourceOperation(self, request, context):
        if (request.nodeId != self.provider.currentOwner
                or not self.provider.operationCallbacks.has_key(request.operation)):
            return distrcom_rpcs_pb2.SROperationReturn(OK=False)

        if request.HasField('args'):
            operation_return = self.provider.operationCallbacks[request.operation](self.provider.SRData, request.args)
        else:
            operation_return = self.provider.operationCallbacks[request.operation](self.provider.SRData)

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
    SRData = None

    # will be the ID of the node currently owning/locking the SR
    currentOwner = None

    operationCallbacks = dict()

    consumers = dict()
    sharedResources = list()

    def __init__(self, initial_sr_data):
        self.SRData = initial_sr_data

    def Start(self, port = SERVER_PORT):
        async def serve():
            global server
            if server is not None:
                raise Exception("Start called twice")

            server = grpc.aio.server()

            distrcom_rpcs_pb2_grpc.add_SharedResourceServicer_to_server(SRProviderHandler(self), server)

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
                        JOIN_TIMEOUT
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

    # Adds an atomic operation on the shared resource
    def AddResourceOperation(self, name, callback):
        self.operationCallbacks[name] = callback