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

while True:
    pass
