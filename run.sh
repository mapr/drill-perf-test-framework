#!/bin/bash
set -x
source PerfTestEnv.conf
source params.conf

# Get the RUNID
if [ ! -e runid.dat ]; then
	echo 2 > runid.dat
	RUNID=1
else
	RUNID=`cat runid.dat`
	echo $((RUNID+1)) > runid.dat
fi

TESTNAME=${RUNID}_${gitCommitId}_${benchmark}_`date +%Y%m%d_%H%M%S`

mkdir -p $PWD/log/$TESTNAME
RunLogDir=$PWD/log/$TESTNAME

### Executing Queries
for queryId in $listOfQueries; do
	#Strip off any list enclosing quotes
	queryId=`echo $queryId | sed "s|\"||g"`
	logdir=$RunLogDir/Q${queryId}
	mkdir -p $logdir
	logFile=$logdir/Q${queryId}.log

	### Restart Drillbits
	if [[ "$useFreshDrillbit" == "yes" ]] 
	then
		echo "[INFO] restarting drillbits ..."
		clush -a "$DRILL_HOME/bin/drillbit.sh restart"
		sleep 10
	fi
	
	### Running a WarmUp query (to load relevant classes into JVM)
	if [[ "$dbitWarmUp" == "yes" ]]; then
		echo "select * from sys.options limit 2;" > warmUp.q
		echo "[INFO] Running a quick warm-up query"
		java -cp ${DRILL_JDBC_CLASSPATH} -Dtimeout=60 -Dconn="jdbc:drill:schema=${testSchema}" PipSQueak warmUp.q	
	fi
	
	### Cleaning up Caches across the cluster
	if [[ "$dropCaches" == "yes" ]]; then
               	echo "[INFO] Cleaning up Caches across the cluster"
		clush -a "echo 3 > /proc/sys/vm/drop_caches"
		sleep 15; #Pause
	fi
	
	### Picking up a Random DrillBit for a query (reused over multiple attempts)
	indx=$RANDOM%`cat drillbits.lst|wc -l`
	indx=$(( indx + 1 ))
        selectedDrillbit=`head -$indx drillbits.lst|tail -1`
        #selectedDrillbit=10.10.101.111

	### TPCH Q15 Hack (as it involves 3 queries)
	if [[ "$benchmark" == "TPCH" ]] && [[ "$queryId" == "15" ]] ; then
		#Running The View Creation 1st
		echo "[INFO] Running create view for TPCH Query 15 ....  "
		java -cp ${DRILL_JDBC_CLASSPATH} -Dalter=${alterations} -Dtimeout=`echo $timeout*60 | bc` -Dconn="jdbc:drill:schema=${testSchema};drillbit=${selectedDrillbit}" PipSQueak $benchmark/Queries/${queryId}a.q | tee -a ${logFile}
		sleep 5; #Pause
	fi
	
	### Run query multiple attempts
	for attempt in `seq 1 ${maxAttempts}`; do
		PROFILE_ID=Q${queryId}_$attempt 
		### Start tracking Resources
		if [[ "$statsCollection" == "yes" ]] ; then
			echo "[INFO] starting stats collections ...."
			clush -a $TestKitDir/utils/startStatsCollection.sh $logdir $PROFILE_ID  &
		fi

		### Run Query
		echo "[INFO] Running Attempt #$attempt for $benchmark query $queryId ...."
		if [[ "$benchmark" == "TPCH" ]] && [[ "$queryId" == "11" ]] ; then
			echo "[INFO] TPCH Q11 is ScaleFactor dependent, constructing the query to reflect the ScaleFactor ..."
			rm -rf 11.q
			cp TPCH/Queries/11.q 11.q
			sed -i "s/ScaleFactor/$scaleFactor/" 11.q
			java -cp ${DRILL_JDBC_CLASSPATH} -Dalter=${alterations} -Dtimeout=`echo $timeout*60 | bc` -Dconn="jdbc:drill:schema=${testSchema};drillbit=${selectedDrillbit}" PipSQueak 11.q |tee -a ${logFile}
			rm -f 11.q
		elif  [[ "$benchmark" == "TPCDS" ]] && [[ "$queryId" == "49" ]] ; then
			echo "alter session set \`planner.enable_decimal_data_type\`=true;" > ${alterations}
			java -cp ${DRILL_JDBC_CLASSPATH} -Dalter=${alterations} -Dtimeout=`echo $timeout*60 | bc` -Dconn="jdbc:drill:schema=${testSchema};drillbit=${selectedDrillbit}" PipSQueak $benchmark/Queries/${queryId}.q |tee -a ${logFile}
			rm -f ${alterations}
		else
			java -cp ${DRILL_JDBC_CLASSPATH} -Dalter=${alterations} -Dtimeout=`echo $timeout*60 | bc` -Dconn="jdbc:drill:schema=${testSchema};drillbit=${selectedDrillbit}" PipSQueak $benchmark/Queries/${queryId}.q |tee -a ${logFile}
		fi

		### Stop tracking Resources, copy the collected stats from remote drillbits and clean up
		if [[ "$statsCollection" == "yes" ]] ; then
			echo "[INFO] stop stats collections and copy the collected stats from remote nodes ...."
			clush -a $TestKitDir/utils/stopStatsCollection.sh 
			$TestKitDir/utils/copyRemoteStats.sh remoteDrillbits $logdir
			clush -g remoteDrillbits "rm -rf $PWD/log/*"
		fi

		### Grab & save Drill profile page
		drillQueryId=`tac  ${logFile} |  grep -m1 QUERYID | cut -f2 -d' '`
		DrillProfileJsonFile=${logdir}/${PROFILE_ID}_id_${drillQueryId}.json
		DrillProfileHTMLFile=${logdir}/${PROFILE_ID}_id_${drillQueryId}.html

		wget -q -O $DrillProfileJsonFile http://${selectedDrillbit}:8047/profiles/${drillQueryId}.json
		wget -q -O $DrillProfileHTMLFile http://${selectedDrillbit}:8047/profiles/${drillQueryId}
		
		sleep 10; #Pause
	done
	
	### TPCH Q15 Hack (as it involves 3 queries)
	if [[ "$benchmark" == "TPCH" ]] && [[ "$queryId" == "15" ]] ; then
		#Running The View Deletion Last
		echo "[INFO] Drop the view for TPCH Query 15 ....  "
		java -cp ${DRILL_JDBC_CLASSPATH} -Dalter=${alterations} -Dtimeout=`echo $timeout*60 | bc` -Dconn="jdbc:drill:schema=${testSchema};drillbit=${selectedDrillbit}" PipSQueak $benchmark/Queries/${queryId}c.q |tee -a ${logFile}
	fi
done
