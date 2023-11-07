import os 
import sys
import subprocess
import time 
from collections import defaultdict
from datetime import datetime
import pandas as pd 
import argparse
import logging
import random

logging.basicConfig(
     level=logging.DEBUG,
     format= '[%(asctime)s] - %(message)s',
     datefmt='%H:%M:%S'
 )

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--frequency", type = float, default = 0.5, help = "How frequently (in seconds) to kill a NameNode.")
parser.add_argument("--how", type = str, default = "round-robin", help = "How to select a NameNode to be killed. Valid options include: \"round-robin\", \"random\"")
parser.add_argument("-d", "--duration", type = float, default = 60, help = "Duration (in seconds) to run the script.")
parser.add_argument("-n", "--num-deployments", dest = "num_deployments", type = int, default = 20, help = "The number of available deployments.")
parser.add_argument("-i", "--initial-delay", dest = "initial_delay", type = int, default = 30, help = "Initial delay (in seconds) before killing the first NameNode.")
parser.add_argument("--sample", action = 'store_true', help = "Use the hard-coded sample input for debugging.")
args = parser.parse_args()

frequency = args.frequency
duration_sec = args.duration
method = args.how 
num_deployments = args.num_deployments
use_sample_input = args.sample
duration_ms = duration_sec * 1000
initial_delay = args.initial_delay

# Requires Python 3.7
def current_milli_time():
    return time.time_ns() // 1_000_000

start_time = datetime.now()
start = current_milli_time()

if frequency <= 0 or frequency > 2:
    logger.error("Frequency must be within the interval (0, 2).")

last_deployment_killed = -1 

def kill_pod(pod: str):
    logger.info("Killing pod '%s' now..." % pod)
    
    if not use_sample_input:
        result = subprocess.run(["kubectl", "delete", "pod", pod], stdout=subprocess.PIPE)
        lines = result.stdout.decode().split("\n")    
    
        logger.info("Output from killing pod:")
        logger.info(lines)
    else:
        logger.info("<PREENDING TO KILL POD '%s'" % pod)

def kill_round_robin(pods: dict, last_deployment_killed = -1, num_deployments = 20):
    """
    Returns:
        int: The deployment of the killed NN.
    """
    logger.info("Selecting NameNode to kill via round-robin.") 
    
    idx = last_deployment_killed + 1
    
    # Wrap around.
    if idx >= num_deployments:
        idx = 0
    
    stop_at = idx 
    
    def search_for_target_pod(candidate_deployment):
        logger.info("Checking deployment %d for candidate pods..." % candidate_deployment)
        
        candidate_pods = pods[candidate_deployment]
        
        if len(candidate_pods) > 0:
            logger.info("Deployment %d has %d candidate pod(s)." % (candidate_deployment, len(candidate_pods)))
            target_pod = random.choice(candidate_pods)
            logger.info("Selected pod '%s' for termination from pool of %d candidate pod(s)." % (target_pod, len(candidate_pods)))
            kill_pod(target_pod)
            return True, candidate_deployment 
        else:
            logger.info("Deployment %d has 0 candidate pods. Skipping." % candidate_deployment)
        
        # Next deployment to check.
        if (candidate_deployment+1) >= num_deployments:
            candidate_deployment = 0 
        else:
            candidate_deployment += 1
        
        return False, candidate_deployment 

    # Emulate a do-while loop.
    namenode_killed, idx = search_for_target_pod(idx)
    while idx != stop_at and not namenode_killed:
        namenode_killed, idx = search_for_target_pod(idx)
    
    if namenode_killed:
        return idx 
    else:
        logger.warn("Could not find a NameNode to kill...")
        return last_deployment_killed
    
def kill_random(pods: dict, last_deployment_killed = -1, num_deployments = 20):
    """
    Returns:
        int: The deployment of the killed NN.
    """
    logger.info("Selecting NameNode to kill via random.") 
    
    candidate_deployments = list(pods.keys())
    target_deployment = random.choice(candidate_deployments)
    
    print("Selected deployment %d for termination from pool of %d candidate deployment(s)." % (target_deployment, len(candidate_deployments)))
    
    candidate_pods = pods[target_deployment]
    target_pod = random.choice(candidate_pods)
    
    logger.info("Selected pod '%s' for termination from pool of %d candidate pod(s)." % (target_pod, len(candidate_pods)))
    
    kill_pod(target_pod)
    
    return target_deployment

method_funcs = {
    "round-robin": kill_round_robin,
    "random": kill_random
}

if method not in method_funcs:
    raise ValueError("Invalid selection method specified: \"%s\"" % method)

sample_input = """
NAME                                                         READY   STATUS    RESTARTS   AGE
owdev-alarmprovider-79b55587cd-nkhp6                         1/1     Running   0          22h
owdev-apigateway-66cb64959-88h6s                             1/1     Running   0          22h
owdev-controller-0                                           1/1     Running   0          22h
owdev-couchdb-7894f89985-rxll4                               1/1     Running   0          22h
owdev-invoker-0                                              1/1     Running   0          5m41s
owdev-invoker-1                                              1/1     Running   0          5m39s
owdev-invoker-2                                              1/1     Running   0          5m36s
owdev-kafka-0                                                1/1     Running   0          22h
owdev-kafkaprovider-6b85d87465-2pthq                         1/1     Running   0          22h
owdev-nginx-5ff7477b4d-4hxxk                                 1/1     Running   0          22h
owdev-redis-76dd89fc6f-jngvc                                 1/1     Running   0          22h
owdev-zookeeper-0                                            1/1     Running   0          22h
wskowdev-invoker-00-1-prewarm-genericnamenode10              1/1     Running   0          5m35s
wskowdev-invoker-00-10-whisksystem-namenode1                 1/1     Running   0          4m58s
wskowdev-invoker-00-11-whisksystem-namenode15                1/1     Running   0          4m58s
wskowdev-invoker-00-12-whisksystem-namenode14                1/1     Running   0          4m58s
wskowdev-invoker-00-13-prewarm-genericnamenode10             1/1     Running   0          3m18s
wskowdev-invoker-00-14-whisksystem-namenode1                 1/1     Running   0          3m18s
wskowdev-invoker-00-15-prewarm-genericnamenode10             1/1     Running   0          54s
wskowdev-invoker-00-16-prewarm-genericnamenode10             1/1     Running   0          47s
wskowdev-invoker-00-17-prewarm-genericnamenode10             1/1     Running   0          27s
wskowdev-invoker-00-18-prewarm-genericnamenode10             1/1     Running   0          24s
wskowdev-invoker-00-19-whisksystem-namenode0                 1/1     Running   0          23s
wskowdev-invoker-00-2-prewarm-genericnamenode10              1/1     Running   0          4m58s
wskowdev-invoker-00-20-whisksystem-namenode0                 1/1     Running   0          23s
wskowdev-invoker-00-21-whisksystem-namenode13                1/1     Running   0          23s
wskowdev-invoker-00-22-whisksystem-namenode0                 1/1     Running   0          23s
wskowdev-invoker-00-23-whisksystem-namenode0                 1/1     Running   0          23s
wskowdev-invoker-00-24-whisksystem-namenode0                 1/1     Running   0          22s
wskowdev-invoker-00-25-whisksystem-namenode0                 1/1     Running   0          22s
wskowdev-invoker-00-26-whisksystem-namenode13                1/1     Running   0          22s
wskowdev-invoker-00-27-prewarm-genericnamenode10             1/1     Running   0          21s
wskowdev-invoker-00-28-whisksystem-namenode1                 1/1     Running   0          21s
wskowdev-invoker-00-29-whisksystem-namenode0                 1/1     Running   0          20s
wskowdev-invoker-00-3-whisksystem-namenode20                 1/1     Running   0          4m58s
wskowdev-invoker-00-30-prewarm-genericnamenode10             1/1     Running   0          19s
wskowdev-invoker-00-31-whisksystem-namenode0                 1/1     Running   0          19s
wskowdev-invoker-00-32-whisksystem-namenode0                 1/1     Running   0          19s
wskowdev-invoker-00-33-whisksystem-namenode0                 1/1     Running   0          19s
wskowdev-invoker-00-34-whisksystem-namenode1                 1/1     Running   0          18s
wskowdev-invoker-00-38-prewarm-genericnamenode10             1/1     Running   0          12s
wskowdev-invoker-00-4-whisksystem-namenode2                  1/1     Running   0          4m58s
wskowdev-invoker-00-5-whisksystem-namenode13                 1/1     Running   0          4m58s
wskowdev-invoker-00-6-whisksystem-namenode0                  1/1     Running   0          4m58s
wskowdev-invoker-00-7-whisksystem-namenode4                  1/1     Running   0          4m58s
wskowdev-invoker-00-8-whisksystem-namenode17                 1/1     Running   0          4m58s
wskowdev-invoker-00-9-whisksystem-namenode16                 1/1     Running   0          4m58s
wskowdev-invoker-11-1-prewarm-genericnamenode10              1/1     Running   0          5m32s
wskowdev-invoker-11-10-whisksystem-namenode19                1/1     Running   0          4m58s
wskowdev-invoker-11-11-whisksystem-namenode12                1/1     Running   0          4m58s
wskowdev-invoker-11-12-whisksystem-namenode11                1/1     Running   0          4m58s
wskowdev-invoker-11-13-prewarm-genericnamenode10             1/1     Running   0          4m
wskowdev-invoker-11-14-whisksystem-namenode8                 1/1     Running   0          3m59s
wskowdev-invoker-11-15-prewarm-genericnamenode10             1/1     Running   0          3m40s
wskowdev-invoker-11-16-whisksystem-namenode12                1/1     Running   0          3m40s
wskowdev-invoker-11-17-prewarm-genericnamenode10             1/1     Running   0          2m59s
wskowdev-invoker-11-18-prewarm-genericnamenode10             1/1     Running   0          58s
wskowdev-invoker-11-19-prewarm-genericnamenode10             1/1     Running   0          23s
wskowdev-invoker-11-2-prewarm-genericnamenode10              1/1     Running   0          4m58s
wskowdev-invoker-11-20-whisksystem-namenode8                 1/1     Running   0          22s
wskowdev-invoker-11-21-prewarm-genericnamenode10             1/1     Running   0          21s
wskowdev-invoker-11-3-whisksystem-namenode18                 1/1     Running   0          4m58s
wskowdev-invoker-11-4-whisksystem-namenode9                  1/1     Running   0          4m58s
wskowdev-invoker-11-5-whisksystem-namenode10                 1/1     Running   0          4m58s
wskowdev-invoker-11-6-whisksystem-namenode8                  1/1     Running   0          4m58s
wskowdev-invoker-11-7-whisksystem-namenode7                  1/1     Running   0          4m58s
wskowdev-invoker-11-8-whisksystem-namenode6                  1/1     Running   0          4m58s
wskowdev-invoker-11-9-whisksystem-namenode21                 1/1     Running   0          4m58s
wskowdev-invoker-22-1-prewarm-genericnamenode10              1/1     Running   0          5m21s
wskowdev-invoker-22-10-whisksystem-namenode20                1/1     Running   0          4m40s
wskowdev-invoker-22-11-whisksystem-namenode7                 1/1     Running   0          4m40s
wskowdev-invoker-22-12-whisksystem-namenode18                1/1     Running   0          4m40s
wskowdev-invoker-22-13-prewarm-genericnamenode10             1/1     Running   0          3m59s
wskowdev-invoker-22-14-prewarm-genericnamenode10             1/1     Running   0          3m40s
wskowdev-invoker-22-15-prewarm-genericnamenode10             1/1     Running   0          3m19s
wskowdev-invoker-22-16-prewarm-genericnamenode10             1/1     Running   0          69s
wskowdev-invoker-22-17-prewarm-genericnamenode10             1/1     Running   0          56s
wskowdev-invoker-22-18-prewarm-genericnamenode10             1/1     Running   0          54s
wskowdev-invoker-22-19-whisksystem-namenode7                 1/1     Running   0          54s
wskowdev-invoker-22-2-whisksystem-invokerhealthtestaction0   1/1     Running   0          5m7s
wskowdev-invoker-22-20-prewarm-genericnamenode10             1/1     Running   0          48s
wskowdev-invoker-22-21-prewarm-genericnamenode10             1/1     Running   0          22s
wskowdev-invoker-22-22-prewarm-genericnamenode10             1/1     Running   0          20s
wskowdev-invoker-22-23-whisksystem-namenode15                1/1     Running   0          19s
wskowdev-invoker-22-27-prewarm-genericnamenode10             1/1     Running   0          9s
wskowdev-invoker-22-3-prewarm-genericnamenode10              1/1     Running   0          4m41s
wskowdev-invoker-22-4-whisksystem-namenode17                 1/1     Running   0          4m40s
wskowdev-invoker-22-5-whisksystem-namenode21                 1/1     Running   0          4m40s
wskowdev-invoker-22-6-whisksystem-namenode15                 1/1     Running   0          4m40s
wskowdev-invoker-22-7-whisksystem-namenode10                 1/1     Running   0          4m40s
wskowdev-invoker-22-8-whisksystem-namenode2                  1/1     Running   0          4m40s
wskowdev-invoker-22-9-whisksystem-namenode5                  1/1     Running   0          4m40s
"""

logger.info("Delaying for %f seconds." % initial_delay)
time.sleep(initial_delay)
logger.info("Starting...")
while (current_milli_time() - start < duration_ms):
    if not use_sample_input:
        result = subprocess.run(["kubectl", "get", "pods"], stdout=subprocess.PIPE)
        lines = result.stdout.decode().split("\n")
    else:
        lines = sample_input.split("\n")
    current_num_nns = 0
    pods_per_deployment = defaultdict(list)
    for line in lines:
        # logger.debug("Processing line \"%s\"" % line)
        if "namenode" not in line or "genericnamenode" in line:
            continue 
    
        line_split = line.split(" ")
        current_num_nns += 1
        try:
            deployment = int(line_split[0][-2:])
        except:
            deployment = int(line_split[0][-1:])
        pods_per_deployment[deployment].append(line_split[0])

    # for i in range(num_deployments):
    #     if i in pods_per_deployment:
    #         logger.info("Deployment %d: %d NameNodes" % (i, len(pods_per_deployment[i])))
    #         #for pod in pods_per_deployment[i]:
    #         #    logger.info("    %s" % pod)
    #     else:
    #         logger.info("Deployment %d: 0 NameNodes" % i)

    now = datetime.now()
    logger.info("%s: There are %d NNs actively running." % (now.strftime("%d/%m/%Y %H:%M:%S"), current_num_nns))
    
    last_deployment_killed = method_funcs[method](pods_per_deployment, last_deployment_killed = last_deployment_killed, num_deployments = num_deployments)

    logger.info("Going to sleep for %.2f seconds.\n\n" % frequency)
    time.sleep(frequency)
    logger.info("Woke up.")