import subprocess
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--output", type = str, default = None, help = "Path of the `config.yaml` output file. Default: \"config.yaml\".")
parser.add_argument("-u", "--user", type = str, default = "ubuntu", help = "Username to include in the config file. Default: \"ubuntu\".")
parser.add_argument("-c", "--hdfs-config-file", dest = "hdfs_site_config_file_path", type = str, default = "/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml", help = "Path to the hdfs-site configuration file. Default: \"/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml\"")
parser.add_argument("-i", "--private-ip", dest = "private_ip", type = str, default = None, help = "Private IPv4 of the primary client/experiment driver. This script does not check that a specified IP is actually valid. By default, the script attempts to resolve this automatically.")
parser.add_argument("-a", "--autoscaling-group-name", dest = "autoscaling_group_name", type = str, default = "lambda_fs_clients_ags", help = "The name of the autoscaling group for the client VMs.")

args = parser.parse_args()
if args.private_ip is None:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    private_ip = s.getsockname()[0]
else:
    private_ip = args.private_ip

autoscaling_group_name = args.autoscaling_group_name
hdfs_site_config_file_path = args.hdfs_site_config_file_path
output_path = args.output 

if output_path is None:
    current_working_directory = os.getcwd()
    
    # Try to guess whether the user ran this from the <root dir>/scripts directory or <root dir>, though they could've run it from somewhere else entirely.
    # If we think they ran it from <root_dir>/scripts, then output the config.yaml in ../config.yaml, which should actually be <root_dir>/config.yaml.
    if current_working_directory.endswith("scripts"):
        output_path = "../config.yaml"
    else:
        output_path = "config.yaml"

print("Private IP: %s" % private_ip)
print("Autoscaling group name: \"%s\"" % autoscaling_group_name)
print("hdfs-site config path: \"%s\"" % hdfs_site_config_file_path)

output = subprocess.check_output(["./get_client_ips.sh", autoscaling_group_name])

if type(output) is bytes:
  output = output.decode()

ips = [ip for ip in output.split('\n') if len(ip) > 0]

print("Discovered %d IP addresses via the get_client_ips.sh script: %s" % (len(ips), str(ips)))

config = \
"""
namenodeEndpoint: hdfs://%s:9000/
hdfsConfigFile: %s
followers:
""" % (private_ip, hdfs_site_config_file_path)

with open(output_path, "w") as f:
  f.write(config)

  for ip in ips:
    f.write("    -\n")
    f.write("        ip: %s\n" % ip)
    f.write("        user: %s\n" % args.user)