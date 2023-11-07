import os 
import sys
import subprocess
import time 
from datetime import datetime
import pandas as pd 
import argparse
import logging

logging.basicConfig(
     level=logging.DEBUG,
     format= '[%(asctime)s] - %(message)s',
     datefmt='%H:%M:%S'
 )

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--interval", type = float, default = 0.5, help = "Interval to check for active NNs (in seconds).")
parser.add_argument("-d", "--duration", type = float, default = 60, help = "Duration (in seconds) to monitor for NNs.")
args = parser.parse_args()

interval = args.interval
duration_sec = args.duration
duration_ms = duration_sec * 1000

# Requires Python 3.7
def current_milli_time():
    return time.time_ns() // 1_000_000

start_time = datetime.now()
start = current_milli_time()

if interval <= 0 or interval > 2:
    logger.error("Interval must be within the interval (0, 2).")

res = []
while (current_milli_time() - start < duration_ms):
  result = subprocess.run(["kubectl", "get", "pods"], stdout=subprocess.PIPE)
  lines = result.stdout.decode().split("\n")
  current_num_nns = 0
  for line in lines:
    if "namenode" not in line:
      continue 
    current_num_nns += 1
  
  now = datetime.now()
  logger.info("%s: %d NNs" % (now.strftime("%d/%m/%Y %H:%M:%S"), current_num_nns))
  
  res.append((current_milli_time(), current_num_nns))

  time.sleep(interval)

df = pd.DataFrame(res, columns = ["time", "nns"])

df.to_csv("./nn_monitor_data/%s_nns.csv" % start_time.strftime("%d-%m-%Y_%H-%M-%S"))
