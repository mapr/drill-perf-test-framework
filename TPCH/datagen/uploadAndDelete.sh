#!/bin/bash

#Params
uploadFile=$1
uploadDest=$2
tblParts=$3

#[Orig] Copy to Hadoop
hadoop fs -copyFromLocal $uploadFile $uploadDest

#change the file name to be with extension "tbl"
if [ $tblParts -gt 1 ]; then
	fname=`basename $uploadFile`
	f1=`echo $fname|cut -d'.' -f 1`
	f2=`echo $fname|cut -d'.' -f 2`
	f3=`echo $fname|cut -d'.' -f 3`
	hadoop fs -mv  $uploadDest/$fname $uploadDest/${f1}.${f3}.$f2
fi

#delete the local file (or named pipe)
rm -f $uploadFile 

