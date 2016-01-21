# Performance Test Framework for Apache Drill

Performance Test Framework for SQL on Hadoop technologies. Currently supports [Apache Drill](http://drill.apache.org/), a schema-free SQL query engine for Hadoop, NoSQL and cloud storage.

The framework is built for regression testing with focus on query performance. Test cases include customized industry standard benchmarks such as TPC-H and TPC-DS. A subset of these tests are used by the Apache Drill community for pre-commit and pre-release criteria.

## Overview
 1. Clone the repository
 2. Configure test environment
 3. Review tests
 4. Build test framework
 5. Execute tests

### Clone the repository
 <pre><code>
 git clone git@github.com:mapr/drill-perf-test-framework.git
 </code></pre>
Refer to [Github documentation](https://help.github.com/articles/cloning-a-repository) on how to clone a repository. 

### Configure test environment
 1. The test framework requires a distributed file system such as HDFS or MapR-FS to be configured. It also requires that Drill services to be setup on a clustered environment. Refer to [Drill documentation](http://drill.apache.org/docs/installing-drill-in-distributed-mode) for details on how to setup Drill.
 2. Ensure passwordless SSH is enabled among the server nodes in the cluster. 
 3. Ensure the following are installed:
	- clush
 <pre><code>
          yum --enablerepo=epel install clustershell
 </code></pre>

          also ensure that /etc/clustershell/group contains appropriate groups, such as 
	       -- "all":  for all the nodes that will run drillbit
               -- "remoteDrillbits": for all the remote nodes that are running drillbits 
	  
	- dstat
 <pre><code>
          yum install -y dstat
 </code></pre>
	
 4. Edit PerfTestEnv.conf to set needed environmental variables
 5. Edit drillbits.lst to contain all the IPs of the drillbit nodes.
 6. Build the databases  
   	- Currently the kit includes data generation scripts for TPCH and TPCDS databases and some queries for those benchmark tests. See READMEs in TPCH/datagen and TPCDS/datagen for how to generate data and build database for those tests (only parquet files are implemented now).
	- If database is already built, ensure the connect string and workspaces are defined in storage plugin as specified in utils/dfs.json_Template.
 7. Copy stats collection scripts to remote drillbit nodes
 <pre><code>
   ./CopyScriptsToRemote.sh
 </code></pre>
 8. Build the driver
 <pre><code>
   cd driver
   ./buildDriver.sh
 </code></pre>

### Review tests
Each test case is specified in a directory structure:
<pre><code>
   benchmark_name (e.g., TPCH, TPCDS)
      |_ datagen
      |_ Queries
 </code></pre>
 datagen contains the needed resources for building the database

### Execute tests
1. Edit params.conf to reflect what to be run.
2. ./run.sh

### logs and results
results will be located at log/\<runid\>\_\<gitCommitId\>\_\<benchmark\>\_\<timestamp\>/
For each query the following metrics are collected, e.g.:
<pre><code>
[STAT] Rows Fetched : 21842
[STAT] Time to load queries : 3 msec
[STAT] Time to register Driver : 632 msec
[STAT] Time to connect : 1045 msec
[STAT] Time to alter session : 0 msec
[STAT] Time to prep Statement  : 3 msec
[STAT] Time to execute query : 24818 msec
[STAT] Time to get query ID : 0 msec
[STAT] Time to fetch 1st Row : 36858 msec
[STAT] Time to fetch All Rows : 37180 msec
[STAT] Time to disconnect : 3 msec
[STAT] TOTAL TIME : 61998 msec
 </code></pre>
along with iostat, vmstat, mpstat, dstat, as well as jstack for Drillbit.
 
