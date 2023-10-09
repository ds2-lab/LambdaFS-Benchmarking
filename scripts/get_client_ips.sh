#!/bin/sh

# Pass as the one and only command-line argument, the name of the autoscaling group for the client VMs.
# If no arguments are specified, then default to "lambdafs_clients_ag" for the autoscaling group name.

if [ -z "$1" ] # Check for empty first argument.
  then
    AUTOSCALING_GROUP_NAME="lambdafs_clients_ag"
else 
    AUTOSCALING_GROUP_NAME=$1
fi

echo "Autoscaling group name: $AUTOSCALING_GROUP_NAME"

aws autoscaling describe-auto-scaling-instances --region us-east-1 --output text --query "AutoScalingInstances[?AutoScalingGroupName=='$AUTOSCALING_GROUP_NAME'].InstanceId" | xargs -n1 aws ec2 describe-instances --instance-ids $ID --region us-east-1 --query "Reservations[].Instances[].PrivateIpAddress" --output text | awk '{print $1}'