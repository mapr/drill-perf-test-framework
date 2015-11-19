#!/bin/bash
##############################################################################
# TPCH Raw Data Generation 
##############################################################################

if [ $# -lt 2 ]; then 
	echo "[ERROR] Insufficient # of params"
	echo "USAGE: `dirname $0`/$0 <drillbits.lst> <scaleFactor>"
	exit 127
fi
drillbitList=$1
scaleFactor=$2

### Check for IP List -- which contains the list of IPs for all the nodes for generating data 
if [ ! -e $drillbitList ]; then
	echo "[ERROR] IP Source file was not found: "$drillbitList
	exit 127
fi

### Init
### Total HostCount
numHosts=`wc -l $drillbitList | cut -f1 -d' '`
echo "[INFO] Number of clients generating: "$numHosts
hostList=$(cat $drillbitList | tr "\n" " ")
hostListArray=( $hostList )


### Check for workload -- a file with one row for a tpc-h table, each row with format <tblCode> <tblName> <#Chunks>
## Assume that all the nodes are uniformi. If no existing workload file is found, will generate one now - 
## #Chunks = numHosts*CoresPerNode.

coresPerNode=`lscpu|grep "^CPU(s)"|awk '{print $2}'`
workFile=workloads/tpch.workload.${scaleFactor}.lst
if [ ! -e $workFile ]; then
	echo "[INFO] generating Workload file: "$workFile
	echo "[INFO] $(( coresPerNode * numHosts )) chunks will be generated for large tables ..."
	echo "r region	1" >$workFile
	echo "n nation	1" >>$workFile
	echo "s supplier	$(( coresPerNode * numHosts ))" >>$workFile
	echo "c customer	$(( coresPerNode * numHosts ))" >>$workFile
	echo "P part	$(( coresPerNode * numHosts ))" >>$workFile
	echo "S partsupp	$(( coresPerNode * numHosts ))" >>$workFile
	echo "O orders	$(( coresPerNode * numHosts ))" >>$workFile
	echo "L lineitem	$(( coresPerNode * numHosts ))" >>$workFile
fi

### Basic Params
remoteGenDir=/root/datagenTPCH
targetVolPath=/tpchRaw
echo "[INFO] Data will be generated locally on each node at a named pipe $remoteGenDir/<tblName.tbl.<chunk#>"
echo "[INFO] Generated data will be streamingly copied to the cluster at "$targetVolPath
echo "[INFO] e.g. lineitem.tbl.10 --> /tpchRaw/SF100/lineitem/lineitem.10.tbl"

#Clear existing workloads
rm -rf writeData-*.sh

# Track which node
counter=0

#Check Dir on HDFS
dataExists=`hadoop fs -du -s ${targetVolPath}/SF${scaleFactor} | awk '{print $1}'`
if [ $dataExists ]; then
	if [ $dataExists -gt 0 ]; then 
		echo "[ERROR]: Location has data ("$dataExists" bytes): ${targetVolPath}/SF"${scaleFactor}
		exit 127
	fi
fi
###
#Creating Root Directory (if not existent)
echo "[INFO] Creating Root Directory (if not existent)"
hadoop fs -mkdir -p ${targetVolPath}/SF${scaleFactor}

### Init Workloads
for worker in $hostList; do
	fileName=writeData-${worker}.sh
	echo "#!/bin/bash" >> $fileName
	echo "  "  >> $fileName
	echo "workDir=\`dirname \$0\`"  >> $fileName
	echo "cd \$workDir"  >> $fileName
	echo "export DSS_PATH=$remoteGenDir" >> $fileName 
	echo "MaxParalThreads=\`lscpu|grep \"^CPU(s)\"|awk '{print \$2}'\`"  >> $fileName
	echo "  "  >> $fileName
done

### Generate Workloads
while read line; do
	params=( $line )
	#Extracting Parameters
	#echo ${params[*]}
	tblCode=${params[0]}
	tblName=${params[1]}
	tblParts=${params[2]}
	echo "====$tblName==="
	hadoop fs -mkdir -p ${targetVolPath}/SF${scaleFactor}/$tblName
	# Assigning workload in round-robin fashion
	partsDone=1
	while [ $partsDone -le $tblParts ]; do
		set -x
		let hostIdx=$counter%$numHosts
		worker=${hostListArray[$hostIdx]}
		set +x
		#echo "write part $partsDone of $tblParts for $tblName (dbgen : $tblCode) on HostID="$worker
		if [ $tblParts -gt 1 ]; then 
			echo "rm -rf $remoteGenDir/${tblName}.tbl.${partsDone}" >> writeData-${worker}.sh
			echo "mkfifo $remoteGenDir/${tblName}.tbl.${partsDone}" >> writeData-${worker}.sh
			echo "$remoteGenDir/dbgen -s $scaleFactor -T $tblCode -S $partsDone -C $tblParts -f &" >> writeData-${worker}.sh
			echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${tblName}.tbl.${partsDone} ${targetVolPath}/SF${scaleFactor}/$tblName $tblParts &" >> writeData-${worker}.sh
		else
			echo "rm -rf $remoteGenDir/${tblName}.tbl" >> writeData-${worker}.sh
			echo "mkfifo $remoteGenDir/${tblName}.tbl" >> writeData-${worker}.sh
			echo "$remoteGenDir/dbgen -s $scaleFactor -T $tblCode -S $partsDone -C $tblParts -f &" >> writeData-${worker}.sh
			echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${tblName}.tbl ${targetVolPath}/SF${scaleFactor}/$tblName $tblParts &" >> writeData-${worker}.sh
		fi
		echo "$remoteGenDir/wait4process.sh dbgen \$MaxParalThreads " >> writeData-${worker}.sh
		echo "   " >> writeData-${worker}.sh

		let partsDone=1+$partsDone
		let counter=1+$counter
	done
done <$workFile;

### Distribute executables
#Dispatching files
for worker in $hostList; do
	echo "wait " >> writeData-${worker}.sh
	ssh $worker "mkdir $remoteGenDir"
	for prereq in dbgen dists.dss uploadAndDelete.sh wait4process.sh ; do
		scp $prereq $worker:$remoteGenDir
	done
done

echo "[INFO] Started Generation @ "`date +%H:%M:%S`
startTime=`date +%s`

### Distribute & Execute workloads
for worker in $hostList; do
	echo "[INFO] Executing DataGen on "${worker}
	chmod 755 writeData-${worker}.sh
	scp writeData-${worker}.sh ${worker}:$remoteGenDir
	ssh ${worker} $remoteGenDir/writeData-${worker}.sh &
done

### Waiting for completion
echo "[INFO] Waiting until completion..."
wait
endTime=`date +%s`
echo "[INFO] Completed Generation @ "`date +%H:%M:%S`
echo "[INFO] Generated and loaded SF"${scaleFactor}" in "`echo $endTime - $startTime |bc`" sec"

