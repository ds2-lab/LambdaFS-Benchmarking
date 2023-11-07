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
import requests
from kazoo.client import KazooClient

logging.basicConfig(
     level=logging.INFO,
     format= '[%(asctime)s] - %(message)s',
     datefmt='%H:%M:%S'
 )

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--frequency", type = float, default = 30, help = "How frequently (in seconds) to kill a NameNode.")
parser.add_argument("--how", type = str, default = "round-robin", help = "How to select a NameNode to be killed. Valid options include: \"round-robin\", \"random\"")
parser.add_argument("-d", "--duration", type = float, default = 60, help = "Duration (in seconds) to run the script.")
parser.add_argument("-n", "--num-deployments", dest = "num_deployments", type = int, default = 20, help = "The number of available deployments.")
parser.add_argument("-i", "--initial-delay", dest = "initial_delay", type = int, default = 30, help = "Initial delay (in seconds) before killing the first NameNode.")
parser.add_argument("--hosts", nargs='+', default=["127.0.0.1:2181"])
parser.add_argument("-e", "--endpoint", type = str, default = "http://internal-abff98336a53646dabc8bdcbdb5b1bd9-571651522.us-east-1.elb.amazonaws.com:80/api/v1/web/whisk.system/default/namenode")
args = parser.parse_args()

frequency = args.frequency
duration_sec = args.duration
method = args.how 
num_deployments = args.num_deployments
hosts = args.hosts
endpoint = args.endpoint

duration_ms = duration_sec * 1000
initial_delay = args.initial_delay

zk = KazooClient(hosts = hosts)
zk.start()

# Requires Python 3.7
def current_milli_time():
    return time.time_ns() // 1_000_000

start_time = datetime.now()
start = current_milli_time()

headers = {
    # Already added when you pass json=
    # 'Content-Type': 'application/json',
    'Authorization': 'Basic 789c46b1-71f6-4ed5-8c54-816aa4f8c502:abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP',
}

json_data = {"TERMINATE": True}

last_deployment_killed = -1 

def get_num_active_nns():
    nodes_per_deployment = []
    total_nns = 0
    for i in range(0, num_deployments):
        groupname = "namenode" + str(i)
        path = "/" + groupname + "/permanent"
        children = zk.get_children(path)
        total_nns += len(children)
        nodes_per_deployment.append(len(children))
    return nodes_per_deployment, total_nns

def kill_node_in_deployment(deployment: int):
    logger.info("Killing NameNode in deployment '%d' now." % deployment)
    
    response = requests.post(
        "http://internal-abff98336a53646dabc8bdcbdb5b1bd9-571651522.us-east-1.elb.amazonaws.com:80/api/v1/web/whisk.system/default/namenode%d" % deployment,
        headers=headers,
        json=json_data,
        verify=False,
    )
    
    logger.info(response)

def kill_round_robin(nodes_per_deployment: list, last_deployment_killed = -1, num_deployments = 20):
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
    
    def search_for_target_deployment(candidate_deployment):
        logger.info("Checking deployment %d for candidate NameNodes..." % candidate_deployment)
        
        candidate_nodes = nodes_per_deployment[candidate_deployment]
        
        if candidate_nodes > 1:
            logger.info("Found target deployment: there are %d active NameNodes in deployment %d." % (candidate_nodes, candidate_deployment))
            kill_node_in_deployment(candidate_deployment)
            return True, candidate_deployment 
        else:
            logger.info("Deployment %d has 0 candidate NameNodes. Skipping." % candidate_deployment)
        
        # Next deployment to check.
        if (candidate_deployment+1) >= num_deployments:
            candidate_deployment = 0 
        else:
            candidate_deployment += 1
        
        return False, candidate_deployment 

    # Emulate a do-while loop.
    namenode_killed, idx = search_for_target_deployment(idx)
    while idx != stop_at and not namenode_killed:
        namenode_killed, idx = search_for_target_deployment(idx)
    
    if namenode_killed:
        return idx 
    else:
        logger.warn("Could not find a NameNode to kill...")
        return last_deployment_killed
    
def kill_random(nodes_per_deployment: list, last_deployment_killed = -1, num_deployments = 20):
    """
    Returns:
        int: The deployment of the killed NN.
    """
    logger.info("Selecting NameNode to kill via random.") 
    
    candidate_deployments = [x for x in range(0, len(nodes_per_deployment)) if nodes_per_deployment[x] > 0]
    target_deployment = random.choice(candidate_deployments)
    
    kill_node_in_deployment(target_deployment)
    
    return target_deployment

method_funcs = {
    "round-robin": kill_round_robin,
    "random": kill_random
}

if method not in method_funcs:
    raise ValueError("Invalid selection method specified: \"%s\"" % method)

logger.info("Delaying for %f seconds." % initial_delay)
time.sleep(initial_delay)
logger.info("Starting...")
while (current_milli_time() - start < duration_ms):
    nodes_per_deployment, current_num_nns = get_num_active_nns()

    now = datetime.now()
    logger.info("%s: There are %d NNs actively running." % (now.strftime("%d/%m/%Y %H:%M:%S"), current_num_nns))
    
    last_deployment_killed = method_funcs[method](nodes_per_deployment, last_deployment_killed = last_deployment_killed, num_deployments = num_deployments)

    logger.info("Going to sleep for %.2f seconds.\n\n" % frequency)
    time.sleep(frequency)
    logger.info("Woke up.")