import subprocess
import sys
import time

SR_COUNT = int(sys.argv[1]) if len(sys.argv)>2 else 1
CONSUMER_COUNT = int(sys.argv[2]) if len(sys.argv)>2 else 5

HOST = '127.0.0.1'
BASE_PORT = 8000

consumer_procs = []
sr_procs = []

for i in range(SR_COUNT):
    sr_procs.append(subprocess.Popen(['konsole', '-e', 'python', 'lib/distrcom_test_sr.py', str(BASE_PORT + i)]))

time.sleep(.1)

for i in range(CONSUMER_COUNT):
    consumer_procs.append(subprocess.Popen(['konsole', '-e', 'python', 'lib/distrcom_test_consumer.py', str(BASE_PORT + SR_COUNT + i), HOST, str(BASE_PORT)]))

while True:
    pass