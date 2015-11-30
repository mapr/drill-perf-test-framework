#!/bin/bash

drillbit=`head -1 ../drillbits.lst`
tmpstmp=`date +%Y%m%d_%H%M%S`

curl -X GET http://${drillbit}:8047/storage/dfs.json > dfs.json.SAVED.$tmstmp
curl -X POST -H "Content-Type: application/json" -d @dfs.json http://${drillbit}:8047/storage/dfs.json
