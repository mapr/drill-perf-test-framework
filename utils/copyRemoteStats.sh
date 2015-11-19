#!/bin/bash
remoteDrillbitList=`clush -g $1 -N hostname -i`
logDir=$2

for h in $remoteDrillbitList
do
scp ${h}:$logDir/* $logDir/ &
done
wait
