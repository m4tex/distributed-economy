import sys
import time
import random
import threading
from lib.distrcom import *

PORT = int(sys.argv[1])
SR_IP = sys.argv[2]
SR_PORT = int(sys.argv[3])
SR_ADDR = f'{SR_IP}:{SR_PORT}'

CONSUME_AMOUNT = 50
MIN_WAIT = 1.0
MAX_WAIT = 5.0

factory = SRConsumer(PORT)
factory.Start(SR_ADDR)

def factory_loop():
    while True:
        # 1. Begär access till SR
        print(f"[F{ PORT }] Requesting SR access...")
        factory.Request(SR_ADDR)

        # 2. När access beviljas (callback från libben), kör consume
        # TODO: quadV's lib måste stödja callbacks, annars simulera med sleep
        time.sleep(0.5)  # placeholder tills libben är klar

        # 3. Släpp access (libben sköter detta)
        # 4. Sov slumpmässigt
        wait = random.uniform(MIN_WAIT, MAX_WAIT)
        print(f"[F{ PORT }] Waiting {wait:.1f}s...")
        time.sleep(wait)

threading.Thread(target=factory_loop, daemon=True).start()

while True:
    pass
