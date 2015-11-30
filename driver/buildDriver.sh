#!/bin/bash


source ../PerfTestEnv.conf 

PipSQueak=PipSQueak
PipSQream=PipSQream
#PipSQuawk=PipSQuawk

if [ -z "${DRILL_HOME}" ] || [ ! -e "${DRILL_HOME}" ]; then
	echo "[ERROR] DRILL_HOME is not defined! Exiting"; exit 127
elif [ ! -e "${PipSQueak}.java" ]; then 
	echo "[ERROR] $PipSQueak.java was not found"; exit 127
elif [ ! -e "${PipSQream}.java" ]; then 
	echo "[ERROR] $PipSQream.java was not found"; exit 127
#elif [ ! -e "${PipSQuawk}.java" ]; then 
#	echo "[ERROR] $PipSQuawk.java was not found"; exit 127
fi

### Compiling
rm -rf  *.class
#javac -cp ${DRILL_HOME} ${PipSQueak}.java ${PipSQream}.java ${PipSQuawk}.java
javac -cp ${DRILL_JDBC_CLASSPATH} -Xlint:deprecation ${PipSQueak}.java ${PipSQream}.java

### Constructing JAR
if [ $? -eq 0 ]; then 
	#jar -czf PipSQueak.jar ${PipSQueak}*.class ${PipSQuawk}*.class
	jar -cvf PipSQueak.jar *.class 
	if [ $? -eq 0 ]; then 
		rm -rf *.class
		echo "Created Executable JAR file: "PipSQueak.jar
	else
		echo "[ERROR] Jar packaging failed!"; exit 127
	fi
else
	echo "[ERROR] Compilation failed!"; exit 127
fi
