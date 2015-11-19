#!/bin/bash

if [ $# -lt 1 ]; then 
	echo "[ERROR] Insufficient # of params"
	echo "USAGE: `dirname $0`/$0 <scaleFactor> "
	exit 127
fi

SF=$1
generateTPCH.sh drillbits.lst $SF 
createCSVViews_TPCH.sh  $SF
ctasDrill_csvToParquet_TPCH.sh $SF
