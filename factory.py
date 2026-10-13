import sys
import time
import random
#import threading
from lib.distrcom import *
import json
import asyncio

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8001
SR_IP = sys.argv[2] if len(sys.argv) > 3 else '127.0.0.1'
SR_PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 8000
SR_ADDR = f'{SR_IP}:{SR_PORT}'

CONSUME_AMOUNT = 50
MIN_WAIT = 1.0
MAX_WAIT = 5.0

wait = random.uniform(MIN_WAIT, MAX_WAIT) # set interval randomly

async def factory_loop():
    while True:
        # 1. Begär access till SR
        #print(f"[F{ PORT }] Requesting SR access...")
        await factory.request(list(factory.sharedResources.keys())[0])

        # 2. När access beviljas (callback från libben), kör consume
        await factory.operation('produce',b'150')
        data = await factory.operation('get_stock')
        print(data)
        print(json.loads(data.decode()))

        # 3. Släpp access (libben sköter detta)
        await factory.free()
        
        # 4. Sov slumpmässigt
        #wait = random.uniform(MIN_WAIT, MAX_WAIT)
        print(f"[Node { factory.nodeId }] Waiting {wait:.1f}s...")
        await asyncio.sleep(wait)

#threading.Thread(target=factory_loop, daemon=True).start()
try:
    factory = SharedResourceConsumer(PORT)
    factory.start(SR_ADDR, factory_loop)
except OSERROR:
    print('meh')
except RuntimeError:
    print('deh')

while True:
    pass
