import sys
from distrcom_lib import *

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8000

provider = SharedResourceProvider(None)
provider.Start(PORT)

initial_data = {
    'stock': 1000,      # antal enheter råvara i lagret
    'produced': 0,      # totalt producerat (för statistik)
    'consumed': 0,      # totalt konsumerat (för statistik)
}

def produce(data, args):
    amount = int(args)
    data['stock'] += amount
    data['produced'] += amount
    print(f"[SR] produce({amount}), stock = {data['stock']}")
    return None  # write-only

def consume(data, args):
    amount = int(args)
    if data['stock'] < amount:
        print(f"[SR] consume({amount}) FAILED, stock = {data['stock']}")
        return b'insufficient'
    data['stock'] -= amount
    data['consumed'] += amount
    print(f"[SR] consume({amount}), stock = {data['stock']}")
    return b'success'

provider = SharedResourceProvider(initial_data)
provider.AddResourceOperation('produce', produce)
provider.AddResourceOperation('consume', consume)
provider.Start(PORT)


while True:
    pass
