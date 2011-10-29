#!/bin/bash

if [ $# -ne 7 ]; then 
  echo "Usage: $0 <finance|tpch|cluster> <timeout> <user> <password> <query dir> <data dir> <output dir>"
  exit 1
fi

workload=$1
if [ "x$workload" != "xtpch" -a "x$workload" != "xfinance" -a "x$workload" != "xcluster" ]; then
  echo "Usage: $0 <finance|tpch|cluster> <timeout> <user> <password> <query dir> <data dir> <output dir>"
  exit 1
fi

# Check arguments.
test -d $5 || (echo "Invalid query base dir: $5" && exit 1)
test -d $6 || (echo "Invalid data dir: $6" && exit 1)
test -d $7 || (mkdir -p $7 && echo "Creating $7 ...")
if [ ! -d $7 ]; then
  echo "Invalid output dir: $7"    
  exit 1
fi

# Set friendly names for arguments, converting relative paths to absolutes.
timeout=$2
user=$3
password=$4
querybase=`cd $5 && pwd`
datadir=`cd $6 && pwd`
outputdir=`cd $7 && pwd`

workingdir=`pwd`

echo "Configuration: "
echo "Timeout: $timeout"
echo "Data dir: $datadir"
echo "Output dir: $outputdir"

# Finance
if [ "x$workload" = "xfinance" ]; then
  querydir=$querybase/finance
  echo "Query dir: $querydir"

  for i in 0; do
    # Create run dir
    rundir=$outputdir/run"$i"
    test -d $rundir || (mkdir -p $rundir && echo "Creating $rundir ...")
    subsdir=`echo $datadir | sed 's/\//\\\\\//g'`

    # Substitute in files.
    sed "s/@@USER@@/$user/; s/@@PASSWORD@@/$password/" \
      < $querydir/schema_definition.oracle.sql > $rundir/schema_definition.oracle.sql
    sed "s/@@PATH@@/$subsdir\//" < $querydir/events.ctl > $rundir/events.ctl

    # Run queries.
    cd $rundir
    sqlplus $user/$password @schema_definition.oracle.sql
    for q in axfinder brokerspread missedtrades pricespread vwap; do
      query_file="$q".sql
      log_file="$q".log
      cp $querydir/$query_file .

      echo "Initializing $q ..."
      sqlplus -S $user/$password @"$querydir"/"$q"_init.sql 
      
      sqlplus $user/$password "@$query_file" &
      pid=$!
      echo "Sleeping for $timeout seconds on pid $pid" && sleep $timeout
      echo "Stopping $pid" && kill -TERM $pid
      cp /tmp/$log_file .

      sleep 60
      echo "Cleaning up $q ..."

      sqlplus -S $user/$password @"$querydir"/"$q"_cleanup.sql &
      sleep 60
    done
    cd $workingdir
  done

# TPCH
else if [ "x$workload" = "xcluster" ]; then

  querydir=$querybase/tpch
  echo "Query dir: $querydir"

  for i in 0; do
    # Create run dir
    rundir=$outputdir/run"$i"
    test -d $rundir || (mkdir -p $rundir && echo "Creating $rundir ...")

    cd $rundir
    
    # Set up DBMS
    cp $querydir/schema_definition.oracle.sql .
    sqlplus $user/$password @schema_definition.oracle.sql
    
    for q in query3 query17 query18 query22 ssb4; do
      query_file="$q".sql
      control_file="$q".ctl
      agenda_file="$q"_agenda.csv
      log_file="$q".log
      subsdir=`echo $datadir | sed 's/\//\\\\\//g'`
      echo "Substituting with: $subsdir"
  
      # Substitute in files.
      sed "s/@@PATH@@/$subsdir/; s/@@AGENDA@@/$agenda_file/" \
        < $querydir/events.ctl > $rundir/$control_file
      
      sed "s/@@USER@@/$user/; s/@@PASSWORD@@/$password/" \
        < $querydir/$query_file > $rundir/$query_file 

      # Truncate any existing events for TPCH queries
      echo "TRUNCATE TABLE AGENDA;" | sqlplus $user/$password
      
      echo "Initializing $q ..."
      sqlplus -S $user/$password @"$querydir"/"$q"_init.sql 

      echo "Loading data for $q ..."      
      time sqlldr $user/$password control=$control_file log="$q"_ldr.log parallel=true silent=feedback
      
      echo "Running $q ..."
      sqlplus $user/$password "@$query_file" &
      pid=$!
      echo "Sleeping for $timeout seconds on pid $pid" && sleep $timeout
      echo "Stopping $pid" && kill -TERM $pid
      cp /tmp/$log_file .
      
      sleep 60
      echo "Cleaning up $q ..."

      sqlplus -S $user/$password @"$querydir"/"$q"_cleanup.sql
      sleep 60

    done
    cd $workingdir    
  done
  
else
  
  querydir=$querybase/tpch
  echo "Query dir: $querydir"

  for i in 0; do
    # Create run dir
    rundir=$outputdir/run"$i"
    test -d $rundir || (mkdir -p $rundir && echo "Creating $rundir ...")

    subsdir=`echo $datadir | sed 's/\//\\\\\//g'`

    # Substitute in files.
    sed "s/@@USER@@/$user/; s/@@PASSWORD@@/$password/" \
      < $querydir/schema_definition.oracle.sql > $rundir/schema_definition.oracle.sql
    sed "s/@@PATH@@/$subsdir\//" < $querydir/events.ctl > $rundir/events.ctl

    # Run queries.
    cd $rundir
    sqlplus $user/$password @schema_definition.oracle.sql
    for q in serverload; do
      query_file="$q".sql
      log_file="$q".log
      cp $querydir/$query_file .

      echo "Initializing $q ..."
      sqlplus -S $user/$password @"$querydir"/"$q"_init.sql 
      
      sqlplus $user/$password "@$query_file" &
      pid=$!
      echo "Sleeping for $timeout seconds on pid $pid" && sleep $timeout
      echo "Stopping $pid" && kill -TERM $pid
      cp /tmp/$log_file .

      sleep 60
      echo "Cleaning up $q ..."

      sqlplus -S $user/$password @"$querydir"/"$q"_cleanup.sql &
      sleep 60
    done
    cd $workingdir
  done
fi
