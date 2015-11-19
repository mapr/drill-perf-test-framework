#!/bin/bash

drillbit=`head -1 ../drillbits.lst`

curl -X GET http://${drillbit}:8047/storage/dfs.json > dfs.json.SAVED
curl -X POST -H "Content-Type: application/json" -d @dfs.json http://${drillbit}:8047/storage/dfs.json
