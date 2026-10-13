import sys
from lib.distrcom import *

PORT = int(sys.argv[1]) if len(sys.argv)>3 else 8001
SR_IP = sys.argv[2] if len(sys.argv) > 3 else '127.0.0.1'
SR_PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 8000

SR_ADDR = f'{SR_IP}:{SR_PORT}'

def resource_granted():
    print('Resource Granted')

consumer = SRConsumer(PORT)
consumer.start(
    SR_ADDR,
    lambda:
        consumer.request(consumer.sharedResources[0], resource_granted)
)

while True:
    pass