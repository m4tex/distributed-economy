import sys
from distrcom import *

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8000

def increment(data):
    data[0] += 1

def read(data):
    return data[0].to_bytes(32, byteorder='big')

provider = SharedResourceProvider([0])
provider.add_resource_operation('increment', increment)
provider.add_resource_operation('read', read)
provider.start(PORT)

while True:
    pass
