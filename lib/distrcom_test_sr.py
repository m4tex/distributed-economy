import sys
from distrcom import *

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8000

provider = SharedResourceProvider(None)
provider.start(PORT)

while True:
    pass
