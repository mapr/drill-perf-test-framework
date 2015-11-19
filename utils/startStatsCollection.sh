#!/bin/bash
logDir=$1
mkdir -p $logDir

RunId=${2}_`hostname -s`

VMSTAT_FILE=$logDir/vmstat-${RunId}.log
MPSTAT_FILE=$logDir/mpstat-${RunId}.log
IOSTAT_FILE=$logDir/iostat-${RunId}.log
DSTAT_FILE=$logDir/dstat-${RunId}.log

echo "Starting vmstat"
vmstat -t 1 &> $VMSTAT_FILE &

echo "Starting iostat"
iostat -dxtcm 1  &> $IOSTAT_FILE &

echo "Starting dstat"
dstat -t -cdngi &> $DSTAT_FILE &

echo "Starting mpstat"
mpstat -P ALL 1 &> $MPSTAT_FILE &

#### Dumping JStack every 30 sec
interval=30
JSTACK_FILE=$logDir/jstack-${RunId}.log
pid4jstack=`jps -m | grep Drillbit| cut -f1 -d' '`
echo "jstack $pid4jstack"


while [ true ]; do
        echo "[TIME] "`date +%Y-%m-%d_%T` >> $JSTACK_FILE
        jstack $pid4jstack >> $JSTACK_FILE
        sleep $interval
done

