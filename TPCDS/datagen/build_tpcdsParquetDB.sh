#!/bin/bash

if [ $# -lt 1 ]; then 
	echo "[ERROR] Insufficient # of params"
	echo "USAGE: `dirname $0`/$0 <scaleFactor> "
	exit 127
fi

SF=$1
generateTPCDS.sh drillbits.lst $SF 
createCSVViews_TPCDS.sh  $SF
ctasDrill_csvToParquet_TPCDS.sh $SF
createParqView_TPCDS.sh  $SF
