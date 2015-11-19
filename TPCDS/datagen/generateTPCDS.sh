#!/bin/bash
##############################################################################
# TPCDS Data Generation 
##############################################################################

if [ $# -lt 2 ]; then 
	echo "[ERROR] Insufficient # of params"
	echo "USAGE: `dirname $0`/$0 <drillbits.lst> <scaleFactor>"
	exit 127
fi
drillbitList=$1
scaleFactor=$2

workDir=`dirname $0`

#set up workspace
cd $TestKitDir/utils
cat dfs.json_Template|sed "s/scalFactor/${scaleFactor}/g" > dfs.json
./set_storage_plugin.sh

cd $workDir

### Check for IP List 
if [ ! -e $drillbitList ]; then
	echo "[ERROR] IP Source file was not found: "$drillbitList
	exit 127
fi
### Check for workload
workFile=workloads/tpcds.workload.${scaleFactor}.lst
if [ ! -e $workFile ]; then
	echo "[ERROR] Workload file was not found: "$workFile
	exit 127
fi

### Basic Params
remoteGenDir=/root/datagenTPCDS
targetVolPath=/tpcdsRaw
echo "[INFO] Data will be generated locally on each node at "$remoteGenDir
echo "[INFO] Generated data will be copied to the cluster at "$targetVolPath
echo "[INFO] e.g. SF100 --> /tpcdsRaw/SF100/<tableName>"
#Clear existing workloads
rm -rf writeData-*.sh

### Init
### Total HostCount
numHosts=`wc -l $drillbitList | cut -f1 -d' '`
echo "[INFO] Number of clients generating: "$numHosts
hostList=$(cat $drillbitList | tr "\n" " ")
hostListArray=( $hostList )
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
	echo "workDir=\`dirname \$0\`"  >> $fileName
	echo "cd \$workDir"  >> $fileName
        echo "export DSS_PATH=$remoteGenDir" >> $fileName
        echo "MaxParalThreads=\`lscpu|grep \"^CPU(s)\"|awk '{print \$2}'\`"  >> $fileName
	echo "      " >> $fileName 
done

### Generate Workloads
while read line; do
	params=( $line )
	#Extracting Parameters
	#echo ${params[*]}
	tblName=${params[0]}
	tblParts=${params[1]}
	echo "====$tblName==="
	hadoop fs -mkdir -p ${targetVolPath}/SF${scaleFactor}/$tblName
	#[DS]::Marking for RETURNS table (SALES and this are generated together)
	if [[ "$tblName" == *"_sales" ]]; then 
		returnTblName=$(echo $tblName | sed "s|_sales|_returns|g")
		hadoop fs -mkdir -p ${targetVolPath}/SF${scaleFactor}/$returnTblName
	else
		returnTblName=""
	fi
	# Assigning workload
	partsDone=1
	while [ $partsDone -le $tblParts ]; do
		set -x
		let hostIdx=$counter%$numHosts
		worker=${hostListArray[$hostIdx]}
		set +x
		
		### Handle MultiPartition Tables
		#Rename for Drill compatibility (Only if more than 1 part)
		if [ $tblParts -gt 1 ]; then 
			#[DS]::
                        echo "rm -rf $remoteGenDir/${tblName}_${partsDone}_${tblParts}.tbl" >> writeData-${worker}.sh
                        echo "mkfifo $remoteGenDir/${tblName}_${partsDone}_${tblParts}.tbl" >> writeData-${worker}.sh
			if [ -n "$returnTblName" ]; then
                       		echo "rm -rf $remoteGenDir/${returnTblName}_${partsDone}_${tblParts}.tbl" >> writeData-${worker}.sh
                        	echo "mkfifo $remoteGenDir/${returnTblName}_${partsDone}_${tblParts}.tbl" >> writeData-${worker}.sh
			fi
			echo "$remoteGenDir/dsdgen -SCALE $scaleFactor -TABLE $tblName -CHILD $partsDone -PARALLEL $tblParts -SUFFIX .tbl -FORCE Y &" >> writeData-${worker}.sh
			echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${tblName}_${partsDone}_${tblParts}.tbl ${targetVolPath}/SF${scaleFactor}/$tblName &" >> writeData-${worker}.sh
			if [ -n "$returnTblName" ]; then
				echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${returnTblName}_${partsDone}_${tblParts}.tbl ${targetVolPath}/SF${scaleFactor}/$returnTblName &" >> writeData-${worker}.sh
			fi
		else
			#[DS]::
			echo "rm -rf $remoteGenDir/${tblName}.tbl" >> writeData-${worker}.sh
                        echo "mkfifo $remoteGenDir/${tblName}.tbl" >> writeData-${worker}.sh
                        if [ -n "$returnTblName" ]; then
                                echo "rm -rf $remoteGenDir/${returnTblName}.tbl" >> writeData-${worker}.sh
                                echo "mkfifo $remoteGenDir/${returnTblName}.tbl" >> writeData-${worker}.sh
                        fi
			echo "$remoteGenDir/dsdgen -SCALE $scaleFactor -TABLE $tblName -SUFFIX .tbl -FORCE Y &" >> writeData-${worker}.sh
			echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${tblName}.tbl ${targetVolPath}/SF${scaleFactor}/$tblName &" >> writeData-${worker}.sh
			#[DS]:: To account for RETURNS
			if [ -n "$returnTblName" ]; then
				echo "$remoteGenDir/uploadAndDelete.sh $remoteGenDir/${returnTblName}.tbl ${targetVolPath}/SF${scaleFactor}/$returnTblName &" >> writeData-${worker}.sh
			fi
		fi

	 	echo "$remoteGenDir/wait4process.sh dsdgen \$MaxParalThreads " >> writeData-${worker}.sh
	 	echo "      " >> writeData-${worker}.sh

		let partsDone=1+$partsDone
		let counter=1+$counter
	done
done <$workFile;

### Distribute executables
#Dispatching files
for worker in $hostList; do
	echo "wait " >> writeData-${worker}.sh
	ssh $worker "mkdir $remoteGenDir"
	for prereq in dsdgen tpcds.idx uploadAndDelete.sh wait4process.sh; do
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
