import sys
from distrcom_lib import *

PORT = int(sys.argv[1]) if len(sys.argv)>1 else 8000

provider = SharedResourceProvider(None)
provider.Start(PORT)

while True:
    pass