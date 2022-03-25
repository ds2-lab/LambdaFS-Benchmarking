#!/usr/bin/env bash

echo "Executing command $3 on $1@$2."
ssh $1@$2 $3