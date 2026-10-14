import sys
from lib.distrcom import *
import json
import threading
import time

#SERVER_IP = '25.12.136.194'
SERVER_IP = '127.0.0.1'

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8000

initial_data = {
    'stock': 1000,      # antal enheter råvara i lagret
    'produced': 0,      # totalt producerat (för statistik)
    'consumed': 0,      # totalt konsumerat (för statistik)
}

# for dict resources
def produce(data, args): # provider.srData => data
    amount = int(args)
    data['stock'] += amount
    data['produced'] += amount
    print(f"[SR] produce({amount}), stock = {data['stock']}")
    return None  # write-only

def consume(data, args):
    amount = int(args)
    if data['stock'] < amount:
        print(f"[SR] consume({amount}), stock = {data['stock']}, FAILED")
        return b'insufficient'
    data['stock'] -= amount
    data['consumed'] += amount
    print(f"[SR] consume({amount}), stock = {data['stock']}")
    return b'success'
    
def get_stock(data):
    return json.dumps(data).encode('utf-8')

provider = SharedResourceProvider(initial_data)
provider.add_resource_operation('produce', produce)
provider.add_resource_operation('consume', consume)
provider.add_resource_operation('get_stock', get_stock)

provider.start(PORT)

#threading.Event().wait() # doesnt allow ctrl+c
try:
    while True:
        time.sleep(3600) # lower cpu usaeg
except KeyboardInterrupt:
    print("Manual shutdown...")