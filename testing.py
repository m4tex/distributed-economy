import asyncio
import grpc
from lib.distrcom import *
from google.protobuf import empty_pb2
from pprint import pprint as pp

# Adds 50 materials to SharedResourceProvider instance
async def add_some_materials():
    # 1. Öppna en asynkron kanal till porten där din SRConsumer lyssnar (t.ex. 8000)
    target_address = '127.0.0.1:8000' 
    
    async with grpc.aio.insecure_channel(target_address) as channel:
        
        # 2. Skapa en stub (klient) bunden till kanalen
        stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(channel)
        
        # Change ownerId to 1 (our nodeId)
        request1 = distrcom_rpcs_pb2.NodeId( # stub.Acquire(request)
            id = 1
        )
        
        # Use it to access 'produce' or 'consume' rpcs 
        request2 = distrcom_rpcs_pb2.SROperation(
            nodeId = 1,
            operation = 'consume',  # 'produce' or 'consume'
            args = b'150'            # amount
        )
        
        print(f"Skickar Acquire till {target_address}...")
        
        response = await stub.Acquire(request1)
        print(f"Svar mottaget! OK={response.OK}")
        
        response = await stub.ResourceOperation(request2)
        print(f"Svar mottaget! OK={response.OK}")
        # kolla SR terminalen

async def experiment():
    # 1. Öppna en asynkron kanal till porten där din SRConsumer lyssnar (t.ex. 50051)
    target_address = '127.0.0.1:8000' 
    
    async with grpc.aio.insecure_channel(target_address) as channel:
        
        #stub = distrcom_rpcs_pb2_grpc.SRConsumerStub(channel)
        stub = distrcom_rpcs_pb2_grpc.SharedResourceStub(channel)
        
        # request = distrcom_rpcs_pb2.NetworkJoinRequest(
            # listeningPort=60061,  # Din egna port som du vill att de ska spara
            # nodeType=distrcom_rpcs_pb2.NodeType.CONSUMER
        # )
        
        # request = empty_pb2.Empty() # stub.RequestResource(request)
        
        # RequestResource does not work
        # request = distrcom_rpcs_pb2.ResourceRequest(
            # nodeId = 1,
            # resourceNode = 1,
            # timestamp = 0
        # )
        
        print(f"Skickar Join till {target_address}...")
        # 4. Gör det asynkrona anropet (await)
        response = await stub.RequestResource(request)
        #response = await stub.Acquire(request)
        print(response)
        print(f"Svar mottaget! OK={response.OK}")

# Kör koden
if __name__ == '__main__':
    asyncio.run(add_some_materials())
    #asyncio.run(experiment())