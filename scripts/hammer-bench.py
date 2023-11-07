import argparse
import io
import time
import os
import random
import datetime
import subprocess
import gevent
from termcolor import colored

import sys
sys.path.append('/home/ben/.local/lib/python3.6/site-packages')

from pssh.clients import ParallelSSHClient

# 1) Run make install-slave  in ~/hammer-bench folder.
# 2) Run python3 hammer-bench.py --sync  to update codes on slaves.
# 3) Run python3 hammer-bench.py --start  to launch slaves.
# 4) Run make bench  in ~/hammer-bench to start benching.
#    After benching, you can
# 5) Run python3 hammer-bench.py --stop  to stop slaves.
#
# Steps 2 and 3 can execute in one command.

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type = str, default = "./client_internal_ips.txt", help = "File containing clients' IPs.")
parser.add_argument("--sync-path", type = str, default = "~/hammer-bench/slave.target", help = "Folder to sync from the master.")
parser.add_argument("--sync-dest", type = str, default = "~/hammer-bench-slave", help = "Folder to sync to on the slaves.")
parser.add_argument("--sync", action = 'store_true', help = "Synchronize the execution files.")
parser.add_argument("--start", action = 'store_true', help = "Start the benchmark.")
parser.add_argument("--stop", action = 'store_true', help = "Stop the benchmark.")
parser.add_argument("--validate", action = 'store_true', help = "Validating the status of the slaves.")
parser.add_argument("--dryrun", action = 'store_true', help = "Run as simulation.")
parser.add_argument("-k", "--key-file", dest = "key_file", type = str, default = "~/.ssh/id_rsa", help = "Path to keyfile.")
parser.add_argument("-u", "--user", type = str, default = "ben", help = "Username for SSH.")
parser.add_argument("--hdfs-site", type = str, dest = "hdfs_site_path", default = "/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml", help = "Location of the hdfs-site.xml file.")
parser.add_argument("--existing_subtree", type = str, dest = "existing_subtree_path", default = "./existing_subtree.txt")

args = parser.parse_args()

sync_path = os.path.expanduser(args.sync_path)
sync_dest = os.path.expanduser(args.sync_dest)
ip_file_path = args.input
key_file = args.key_file
user = args.user
hdfs_site_path = args.hdfs_site_path
existing_subtree_path = args.existing_subtree_path

hosts = []
hosts_no_local = []
with open(ip_file_path, 'r') as ip_file:
    hosts = [x.strip() for x in ip_file.readlines()]
    hosts_no_local = [host for host in hosts if host != "10.150.0.10"]
    print("Hosts: %s" % str(hosts))
    print("Hosts (no local): %s" % str(hosts_no_local))

client = ParallelSSHClient(hosts)
client_sync = ParallelSSHClient(hosts_no_local)

output = None
if args.sync:
    print("Synchronize execution files form {} to {}".format(sync_path, sync_dest))
    client_sync.run_command("mkdir -p {}".format(sync_dest), stop_on_errors=False)
    greenlet = client_sync.copy_file(sync_path, sync_dest, recurse=True)
    gevent.joinall(greenlet, raise_error=True)

    print("Next, copying hdfs-site.xml configuration file...")
    greenlet = client_sync.copy_file(hdfs_site_path, hdfs_site_path, recurse=True)
    gevent.joinall(greenlet, raise_error=True)

    print("Next, copying existing_subtree.txt configuration file...")
    greenlet = client_sync.copy_file(existing_subtree_path, sync_dest + "/existing_subtree.txt", recurse=True)
    gevent.joinall(greenlet, raise_error=True)

    output = list()

if args.start:
    print("Starting the slaves.")
    os.system("sed -i -e 's|^\\(benchmark.dryrun=\\).*|\\1{}|' ~/hammer-bench/master.properties".format("true" if args.dryrun else "false"))
    os.system("sed -i -e 's|^\\(list.of.slaves=\\).*|\\1{}|' ~/hammer-bench/master.properties".format(",".join(hosts)))
    cmd = "cd {} && make bench".format(sync_dest)
    print("Executing command: %s" % cmd)
    output = client.run_command(cmd, stop_on_errors=False)
elif args.stop:
    print("Stopping the slaves.")
    # Process list may include "bash" command itself.
    # This script format command as "cmd pid" first, then grep by asserting the command start with "java", and finally print out the pid."
    output = client.run_command("kill -9 `ps aux | grep io.hops.experiments.controller.Slave | awk '{ print $11\" \"$2 }' | grep '^java' | awk '{print $2}'`")
elif args.validate:
    print("Validating the slaves.")
    output = client.run_command("ps aux | grep io.hops.experiments.controller.Slave | awk '{ printf \"%s %s\", $11, $2; for(i=11;i<=NF;i++){printf \" %s\", $i}; printf \"\\n\"}' | grep '^java'")
elif output == None:
    print("[WARNING] Neither '--sync' nor '--start' was specified. Doing nothing.")

for i in range(len(output)):
    host_output = output[i]
    for line in host_output.stdout:
        print(line)
