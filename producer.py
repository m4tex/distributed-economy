import sys
import time
import random
import threading
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

fail_check_interval = 30.0
fail_chance = 0.5
critical_fail_chance = 0.02

wait = int(sys.argv[4]) if len(sys.argv) > 4 else random.uniform(MIN_WAIT, MAX_WAIT)  # set interval randomly

async def factory_loop():
    last_fail_check = time.time()
    while True:
        
        # 1. Begär access till SR
        await factory.request(list(factory.sharedResources.keys())[0])

        # 2. När access beviljas (callback från libben), kör consume
        await factory.operation('consume',b'150')
        data = await factory.operation('get_stock')
        #print(data)
        #print(json.loads(data.decode()))

        # 3. Släpp access (libben sköter detta)
        await factory.free()
        
        # 4. Sov slumpmässigt
        #wait = random.uniform(MIN_WAIT, MAX_WAIT)
        print(f"[Node { factory.nodeId }] Waiting {wait:.1f}s...")
        await asyncio.sleep(wait)

#threading.Thread(target=factory_loop, daemon=True).start()
factory = SharedResourceConsumer(PORT)
factory.start(SR_ADDR, factory_loop)

#threading.Event().wait()
try:
    while True:
        time.sleep(3600) # lower cpu usaeg
except KeyboardInterrupt:
    print("Manual shutdown...")
