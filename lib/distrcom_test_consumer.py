import asyncio
import sys
from lib.distrcom import *

PORT = int(sys.argv[1]) if len(sys.argv)>3 else 8001
SR_IP = sys.argv[2] if len(sys.argv) > 3 else '127.0.0.1'
SR_PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 8000

SR_ADDR = f'{SR_IP}:{SR_PORT}'

async def cb_on_joined():
    await consumer.request(list(consumer.sharedResources.keys())[0])
    await asyncio.sleep(1)
    await consumer.free()

consumer = SharedResourceConsumer(PORT)
consumer.start(
    SR_ADDR,
    cb_on_joined
)

while True:
    pass